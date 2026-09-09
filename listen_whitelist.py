#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
白名单监听 Demo（支持运行中动态增删监听对象）
------------------------------------------------------------
独立脚本，复用 wxbot_core 的配置加载器读取 config/config.json 里的
白名单联系人（listen_list）与群（group），用 wxautox4 的回调推送模式
注册监听，收到消息后打印，并异步投递到 Kafka（见 kafka_sink.py），
不做任何 AI 回复 / 指令处理 / 转发。

Kafka / Redis 配置（config.json，缺省用脚本内默认值，改动需重启）：
  "kafka_brokers":        "kafka.mw.svc.dev.local:9092",
  "kafka_topic":          "com.jsecode.wxbot.messages.receive",
  "redis_url":            "redis://redis.mw.svc.dev.local:6379/0",
  "redis_listen_channel": "com.jsecode.wxbot.listen.update",
  "redis_password":       ""   # 也可直接写进 redis_url: redis://:pass@host:6379/0

动态修改监听名单，三种方式（都是全量对账，无需重启）：
  1. 往 Redis channel 发全量快照 JSON：
       {"listen_list": ["联系人A"], "group": ["群1", "群2"]}
     订阅线程收到后交主线程 reconcile。
  2. 直接编辑 config/config.json 的 listen_list / group 后保存（mtime 轮询）。
  3. 打开面板 (python web_server.py) 在监听名单里增删（本质同 2）。
Redis 快照与 config.json 是「谁后到谁生效」；收到快照后会刷新 mtime 基准，
避免紧接着被 config.json 回滚。

用法：
    python listen_whitelist.py

前置条件与主程序一致：Windows + 已登录并打开（未最小化）的微信 PC 客户端，
wxautox4 已激活，屏幕缩放 100%。名称需与微信里显示的会话名完全一致。
Ctrl+C 退出。
"""

import os
import queue
import sys
import time
from datetime import datetime

# 复用主项目的配置加载器（会读取 config/config.json 并同步到属性）
from wxbot_core import WXBotConfig

# 直接使用 wxautox4，不走 WXBot 那套 AI/指令流水线
from wxautox4 import WeChat

from kafka_sink import KafkaSink
from redis_control import RedisListenControl

POLL_INTERVAL = 3  # 秒，config.json 变更检测间隔

# config.json 未配置时的默认值
DEFAULT_KAFKA_BROKERS = "kafka.mw.svc.dev.local:9092"
DEFAULT_KAFKA_TOPIC = "com.jsecode.wxbot.messages.receive"
DEFAULT_REDIS_URL = "redis://redis.mw.svc.dev.local:6379/0"
DEFAULT_REDIS_CHANNEL = "com.jsecode.wxbot.listen.update"

_sink = None          # KafkaSink 实例，main() 里初始化
_bot_id = "?"         # 当前登录微信昵称，main() 里赋值

# Redis channel 下发的目标集合队列：订阅线程写，主线程读后 reconcile
# （wxautox 的 AddListenChat/RemoveListenChat 必须在持有 WeChat 对象的主线程调）
_target_q = queue.Queue()


def kafka_settings(cfg):
    """从 config.json 读 kafka 配置，缺省用默认值。启动时读一次，改动需重启。"""
    raw = cfg.config
    brokers = (raw.get("kafka_brokers") or "").strip() or DEFAULT_KAFKA_BROKERS
    topic = (raw.get("kafka_topic") or "").strip() or DEFAULT_KAFKA_TOPIC
    return brokers, topic


def redis_settings(cfg):
    """从 config.json 读 redis 配置，缺省用默认值。启动时读一次，改动需重启。"""
    raw = cfg.config
    url = (raw.get("redis_url") or "").strip() or DEFAULT_REDIS_URL
    channel = (raw.get("redis_listen_channel") or "").strip() or DEFAULT_REDIS_CHANNEL
    password = (raw.get("redis_password") or "").strip() or None
    return url, channel, password


def _mask_url(url):
    """隐藏 url 里 user:pass@ 段，避免密码打进日志。"""
    import re
    return re.sub(r"://[^/@]*@", "://***@", url or "")


def desired_targets(cfg):
    """重新读配置，返回本轮应监听的对象列表（listen_list + group，去重保序）。"""
    cfg.refresh_config()  # = load_config() + update_global_config()
    names = list(cfg.listen_list or []) + list(cfg.group or [])
    if cfg.AllListen_switch:
        print("[提示] config.json 的 AllListen_switch=True（全局模式），"
              "本脚本仍按白名单只监听 listen_list + group。")
    return list(dict.fromkeys(n for n in names if n))


def connect_wechat():
    """连接正在运行的微信客户端，国内版失败则尝试国际版。"""
    try:
        return WeChat(version='微信')
    except Exception:
        print("[警告] 以 '微信' 初始化失败，尝试国际版 'WeChat' ...")
        return WeChat(version='WeChat')


def on_message(msg, chat):
    """
    wxautox4 监听回调：每当已注册会话收到新消息时自动调用。
    :param msg:  消息对象，含 type / attr / sender / content 等
    :param chat: 会话子窗口对象，chat.who 为会话名
    """
    now = datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    who = getattr(chat, 'who', '?')
    # attr: friend=别人发来  self=自己发的(多端同步)  system=系统消息

    # 投递到 Kafka：非阻塞、异常不外抛（否则 wxautox 回调出错会停掉整个监听）
    if _sink is not None:
        try:
            _sink.emit({
                "bot": _bot_id,
                "chat": who,                          # 作为 Kafka key，同会话进同一分区
                "sender": msg.sender,
                "attr": msg.attr,                     # friend / self / system
                "type": msg.type,                     # text / image / voice / quote ...
                "content": msg.content,               # 图片/语音可能是本地路径或占位符
                "msg_id": getattr(msg, "id", None),   # 下游按此去重
                "ts": now.isoformat(timespec="seconds"),
            })
        except Exception as e:
            print(f"[on_message] emit 出错: {e!r}", flush=True)

    print(
        f"[{ts}] 会话:{who} | 属性:{msg.attr} | 类型:{msg.type} | "
        f"发送人:{msg.sender}\n    内容: {msg.content}\n",
        flush=True,
    )


def add_listen(wx, name):
    """注册单个监听，返回是否成功。异常/失败都不抛出，只打印。"""
    try:
        result = wx.AddListenChat(nickname=name, callback=on_message)
    except Exception as e:
        print(f"  ! 监听失败 {name}: {e!r}")
        if 'MoveWindow' in repr(e) or '1400' in repr(e):
            print(f"    ↳ 该昵称多半在「{wx.nickname}」里找不到对应会话（名称需与微信里完全一致），"
                  f"或微信主窗口被最小化/不可见，wxautox4 无法弹出独立聊天窗口。")
        return False
    # AddListenChat 正常时返回真值，失败时可能返回 False 或 {'message': ...}
    if result:
        print(f"  + 已监听 {name}")
        return True
    msg = result.get('message', result) if isinstance(result, dict) else result
    print(f"  ! 监听失败 {name}: {msg}")
    return False


def remove_listen(wx, name):
    """移除单个监听。"""
    try:
        wx.RemoveListenChat(name)
        print(f"  - 已取消监听 {name}")
    except Exception as e:
        print(f"  ! 取消监听失败 {name}: {e!r}")


def _drain_latest(q):
    """取空队列，返回最后一个元素（没有则 None）。用于只应用最新的全量快照。"""
    item = None
    try:
        while True:
            item = q.get_nowait()
    except queue.Empty:
        pass
    return item


def reconcile(wx, registered, want):
    """把当前已注册集合 registered 调整为目标集合 want（原地修改 registered）。"""
    for name in [n for n in want if n not in registered]:
        time.sleep(0.5)
        if add_listen(wx, name):
            registered.add(name)
    for name in [n for n in registered if n not in want]:
        time.sleep(0.3)
        remove_listen(wx, name)
        registered.discard(name)


def main():
    global _sink, _bot_id

    cfg = WXBotConfig()
    config_path = cfg.CONFIG_FILE
    targets = desired_targets(cfg)
    if not targets:
        print("白名单为空：config.json 的 listen_list 和 group 都没有内容。"
              "脚本仍会运行，等你往里加。")

    wx = connect_wechat()
    _bot_id = wx.nickname
    print(f"已连接微信：{wx.nickname}")

    brokers, topic = kafka_settings(cfg)
    print(f"Kafka -> brokers={brokers}  topic={topic}")
    _sink = KafkaSink(brokers=brokers, topic=topic)
    _sink.start()

    redis_url, redis_channel, redis_password = redis_settings(cfg)
    _pw_hint = "（密码：url 内）" if not redis_password else "（密码：redis_password）"
    print(f"Redis -> url={_mask_url(redis_url)}  channel={redis_channel} {_pw_hint}")

    def on_snapshot(listen_list, group):
        # 订阅线程回调：只把全量目标集合塞进队列，reconcile 交给主线程
        want = set(n for n in list(listen_list) + list(group) if n)
        _target_q.put(("redis", want))

    control = RedisListenControl(redis_url, redis_channel, on_snapshot, password=redis_password)
    control.start()

    wx.StopListening()
    time.sleep(1)
    wx.StartListening()

    registered = set()
    reconcile(wx, registered, set(targets))

    try:
        last_mtime = os.path.getmtime(config_path)
    except OSError:
        last_mtime = 0
    last_cfg_check = 0.0

    print(f"\n开始监听，当前 {len(registered)} 个对象：{sorted(registered)}")
    print(f"（改 {config_path} 的 listen_list / group，或往 Redis channel "
          f"{redis_channel} 发全量快照，均可动态更新；Ctrl+C 退出）\n")

    try:
        while True:
            time.sleep(1)

            # 1) 优先处理 Redis channel 下发的全量快照（只取最新一条）
            snap = _drain_latest(_target_q)
            if snap is not None:
                src, want = snap
                print(f"[{datetime.now():%H:%M:%S}] 收到 {src} 全量快照，重新对账监听名单...")
                reconcile(wx, registered, want)
                # 快照即当前真值：mtime 基准也刷新，避免紧接着又被 config.json 覆盖
                try:
                    last_mtime = os.path.getmtime(config_path)
                except OSError:
                    pass
                print(f"  当前监听 {len(registered)} 个对象：{sorted(registered)}\n")
                continue

            # 2) 其次处理 config.json 变更（按 POLL_INTERVAL 节流）
            now_ts = time.time()
            if now_ts - last_cfg_check < POLL_INTERVAL:
                continue
            last_cfg_check = now_ts
            try:
                mtime = os.path.getmtime(config_path)
            except OSError:
                continue
            if mtime == last_mtime:
                continue
            last_mtime = mtime
            print(f"[{datetime.now():%H:%M:%S}] 检测到 config.json 变更，重新对账监听名单...")
            try:
                want = set(desired_targets(cfg))
            except Exception as e:
                print(f"  ! 读取配置失败，跳过本次: {e!r}")
                continue
            reconcile(wx, registered, want)
            print(f"  当前监听 {len(registered)} 个对象：{sorted(registered)}\n")
    except KeyboardInterrupt:
        print("\n收到退出信号，停止监听...")
    finally:
        try:
            control.stop()
        except Exception:
            pass
        try:
            wx.StopListening()
        except Exception:
            pass
        if _sink is not None:
            _sink.stop()   # flush 队列里未发送的消息
    return 0


if __name__ == '__main__':
    sys.exit(main())
