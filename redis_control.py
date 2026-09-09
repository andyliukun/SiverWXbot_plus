#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通过 Redis pub/sub channel 下发监听名单的「全量快照」。

消息格式（JSON，全量，不是增量）：
    {"listen_list": ["联系人A", "联系人B"], "group": ["群1", "群2"]}
缺失的键按空列表处理；多余的键忽略。

用法：
    from redis_control import RedisListenControl
    ctl = RedisListenControl(url, channel, on_snapshot=lambda ll, gg: ..., password="xxx")
    ctl.start()
    ...
    ctl.stop()

password 单独传时优先于 url 里的密码；也可直接写进 url：
    redis://:yourpassword@host:6379/0
ACL 用户名（Redis 6+）请写进 url：redis://user:pass@host:6379/0

回调 on_snapshot(listen_list, group) 在 Redis 订阅线程里执行，
不要在回调里直接调 wxautox 的 UI API（跨线程）；应把目标集合丢进队列，
由持有 WeChat 对象的主线程去 reconcile。

依赖：pip install redis
"""

import json
import threading

try:
    import redis
except ImportError:
    redis = None

RECONNECT_DELAY = 5  # 秒


class RedisListenControl:
    def __init__(self, url, channel, on_snapshot, password=None):
        self.url = url
        self.channel = channel
        self.on_snapshot = on_snapshot
        self.password = password or None
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if redis is None:
            print("[RedisControl] 未安装 redis 库，channel 下发功能不可用", flush=True)
            return
        self._thread = threading.Thread(target=self._run, name="RedisControl", daemon=True)
        self._thread.start()

    def stop(self, timeout=8):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    # ---------------------------------------------------------

    def _run(self):
        while not self._stop.is_set():
            client = pubsub = None
            try:
                kwargs = dict(
                    decode_responses=True,
                    socket_timeout=5, socket_keepalive=True,
                    health_check_interval=30,
                )
                if self.password:
                    kwargs["password"] = self.password   # 优先于 url 里的密码
                client = redis.Redis.from_url(self.url, **kwargs)
                client.ping()
                pubsub = client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.channel)
                print(f"[RedisControl] 已订阅 channel: {self.channel}", flush=True)
                while not self._stop.is_set():
                    msg = pubsub.get_message(timeout=1.0)
                    if msg and msg.get("type") == "message":
                        self._handle(msg.get("data"))
            except Exception as e:
                if self._stop.is_set():
                    break
                print(f"[RedisControl] 连接/订阅出错，{RECONNECT_DELAY}s 后重连: {e!r}", flush=True)
                self._stop.wait(RECONNECT_DELAY)
            finally:
                for obj in (pubsub, client):
                    try:
                        obj and obj.close()
                    except Exception:
                        pass
        print("[RedisControl] 已停止", flush=True)

    def _handle(self, raw):
        try:
            data = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            listen_list = [str(x).strip() for x in (data.get("listen_list") or []) if str(x).strip()]
            group = [str(x).strip() for x in (data.get("group") or []) if str(x).strip()]
        except Exception as e:
            print(f"[RedisControl] 消息解析失败: {e!r}  raw={raw!r}", flush=True)
            return
        print(f"[RedisControl] 收到全量快照  listen_list={listen_list}  group={group}", flush=True)
        try:
            self.on_snapshot(listen_list, group)
        except Exception as e:
            print(f"[RedisControl] on_snapshot 回调出错: {e!r}", flush=True)
