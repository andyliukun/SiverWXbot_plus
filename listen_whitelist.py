#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
白名单监听 Demo
------------------------------------------------------------
用 wxautox4 的回调推送模式监听指定联系人/群：
  - 收到的消息 → 打印 + 异步投递到 Kafka 入站 topic（kafka_sink.py）
  - 消费 Kafka 出站 topic 的发送指令 → 主线程 chat.SendMsg（kafka_source.py）
    指令：{"seq": "123", "appId": "crm", "who": "张三", "sendTime": "2026-09-09T17:00:00",
           "text": "你好", "at": ["李四"], "files": ["D:/a.png"]}
    seq / appId 必填；缺任一个仍尝试发送，但不回结果（无法对齐）
    sendTime 建议带（ISO8601 或 epoch 秒/毫秒）；距今超过 send_max_delay_seconds
    （默认 7200，即 2h）则不发
  - 仅当 seq 和 appId 都有时，把结果投递到 kafka_send_result_topic：
    {"seq","appId","bot","who","ok","skipped","error","via","sendTime","delaySeconds","ts"}
不做任何 AI 回复 / 关键词 / 转发逻辑。

配置文件 listen_whitelist.json（与本脚本同目录，独立于 config/config.json，
不受 web_server.py / 面板影响；启动读一次，改动需重启）：
  {
    "kafka_brokers":            "kafka.mw.svc.dev.local:9092",
    "kafka_topic":              "com.jsecode.wxbot.messages.receive",      # 入站消息
    "kafka_send_topic":         "com.jsecode.wxbot.messages.send",         # 出站发送指令
    "kafka_send_result_topic":  "com.jsecode.wxbot.messages.send.result",  # 发送结果
    "kafka_group_id":           "wxbot-sender",
    "send_max_delay_seconds":   7200,   # sendTime 距今超过则不发（仍回结果）
    "redis_url":                "redis://redis.mw.svc.dev.local:6379/0",
    "redis_listen_channel":     "com.jsecode.wxbot.listen.update",
    "redis_password":           ""      # 也可直接写进 redis_url: redis://:pass@host:6379/0
  }
缺失的键用脚本内默认值。样板见 listen_whitelist.example.json。

监听名单不来自任何配置文件：
  - 唯一来源是 Redis channel 下发的「全量快照」JSON：
      {"listen_list": ["联系人A"], "group": ["群1", "群2"]}
  - 每次收到快照，主线程 reconcile 监听并把快照写入本地状态文件
      listen_targets.json（与本脚本同目录）
  - 启动时先加载该状态文件恢复上次名单；文件不存在则空跑，等第一条 Redis 快照

用法：
    python listen_whitelist.py

前置条件与主程序一致：Windows + 已登录并打开（未最小化）的微信 PC 客户端，
wxautox4 已激活，屏幕缩放 100%。名称需与微信里显示的会话名完全一致。
Ctrl+C 退出。
"""

import json
import os
import queue
import sys
import tempfile
import time
from datetime import datetime

# import wxbot_core 只为触发它对 wxautox 的 WxParam 调优（MESSAGE_HASH /
# CHAT_WINDOW_SIZE 等），与主程序行为一致。不实例化 WXBotConfig，不碰 config.json。
import wxbot_core  # noqa: F401

# 直接使用 wxautox4，不走 WXBot 那套 AI/指令流水线
from wxautox4 import WeChat

from kafka_sink import KafkaSink
from kafka_source import KafkaSource
from redis_control import RedisListenControl


def _base_dir():
    """脚本所在目录；打包成 onefile 时为 exe 所在目录。"""
    if hasattr(sys, "_MEIPASS"):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


CONFIG_PATH = os.path.join(_base_dir(), "listen_whitelist.json")
STATE_PATH = os.path.join(_base_dir(), "listen_targets.json")

# listen_whitelist.json 未配置时的默认值
DEFAULTS = {
    "kafka_brokers": "kafka.mw.svc.dev.local:9092",
    "kafka_topic": "com.jsecode.wxbot.messages.receive",              # 入站：收到的消息
    "kafka_send_topic": "com.jsecode.wxbot.messages.send",            # 出站：发送指令
    "kafka_send_result_topic": "com.jsecode.wxbot.messages.send.result",  # 出站：发送结果
    "kafka_group_id": "wxbot-sender",
    "send_max_delay_seconds": 2 * 3600,   # sendTime 距今超过这个值就不发
    "redis_url": "redis://redis.mw.svc.dev.local:6379/0",
    "redis_listen_channel": "com.jsecode.wxbot.listen.update",
    "redis_password": "",
}

_sink = None          # KafkaSink 实例，main() 里初始化
_bot_id = "?"         # 当前登录微信昵称，main() 里赋值

# Redis channel 下发的全量快照队列：订阅线程写，主线程读后 reconcile + 落盘
# （wxautox 的 AddListenChat/RemoveListenChat 必须在持有 WeChat 对象的主线程调）
_target_q = queue.Queue()

# Kafka 出站发送指令队列：消费线程写，主线程读后 SendMsg
# （SendMsg 同样是 UI/COM 操作，必须在持有 WeChat 对象的主线程调）
_send_q = queue.Queue()


def load_config():
    """读 listen_whitelist.json，缺失的键用 DEFAULTS 补齐。文件不存在也照跑。"""
    cfg = dict(DEFAULTS)
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            cfg.update({k: v for k, v in data.items() if v not in (None, "")})
    except FileNotFoundError:
        print(f"[config] {CONFIG_PATH} 不存在，全部用默认值", flush=True)
    except Exception as e:
        print(f"[config] 读取 {CONFIG_PATH} 失败，全部用默认值: {e!r}", flush=True)
    return cfg


def kafka_settings(cfg):
    """从 listen_whitelist.json 取 kafka 相关配置。"""
    return (
        str(cfg["kafka_brokers"]).strip(),
        str(cfg["kafka_topic"]).strip(),
        str(cfg["kafka_send_topic"]).strip(),
        str(cfg["kafka_send_result_topic"]).strip(),
        str(cfg["kafka_group_id"]).strip(),
    )


def redis_settings(cfg):
    """从 listen_whitelist.json 取 redis 相关配置。"""
    url = str(cfg["redis_url"]).strip()
    channel = str(cfg["redis_listen_channel"]).strip()
    password = (str(cfg.get("redis_password") or "").strip()) or None
    return url, channel, password


def _mask_url(url):
    """隐藏 url 里 user:pass@ 段，避免密码打进日志。"""
    import re
    return re.sub(r"://[^/@]*@", "://***@", url or "")


# ---------------- 监听名单状态文件 ----------------

def _clean_names(seq):
    """去空白、去空串、去重保序。"""
    out = []
    for x in seq or []:
        if not isinstance(x, str):
            continue
        s = x.strip()
        if s and s not in out:
            out.append(s)
    return out


def load_state(path):
    """加载状态文件，返回 (listen_list, group)。不存在/损坏则返回 ([], [])。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _clean_names(data.get("listen_list")), _clean_names(data.get("group"))
    except FileNotFoundError:
        return [], []
    except Exception as e:
        print(f"[state] 读取 {path} 失败，按空名单启动: {e!r}", flush=True)
        return [], []


def save_state(path, listen_list, group):
    """把快照原子写入状态文件（同目录 tmp + os.replace）。"""
    payload = {
        "listen_list": _clean_names(listen_list),
        "group": _clean_names(group),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".listen_targets.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"[state] 写入 {path} 失败: {e!r}", flush=True)
        try:
            os.unlink(tmp)
        except OSError:
            pass


# ---------------- 微信 / 监听 ----------------

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


def _parse_send_time(v):
    """把 sendTime 解析成本地无时区 datetime；无法解析返回 None。
    支持：ISO8601 字符串（可带 Z / 时区偏移）、epoch 秒、epoch 毫秒。"""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        sec = v / 1000.0 if v > 1e12 else float(v)
        try:
            return datetime.fromtimestamp(sec)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(v, str):
        s = v.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt
    return None


def do_send(wx, registered, cmd, max_delay_seconds):
    """
    执行一条来自 Kafka 出站 topic 的发送指令。只在主线程调。
    cmd: {"seq": 必填(消息id), "appId": 必填, "who": 必填, "sendTime": 建议带,
          "text": 可选, "at": str|list 可选, "files": list 可选}
    - who 已被监听时用子窗口 chat.SendMsg；否则退回主窗口 wx.SendMsg(who=...)。
    - sendTime 距当前时间超过 max_delay_seconds（默认 2h）则不发。
    返回值：
      dict  -> seq 和 appId 都有：发送结果，调用方投递到 result topic
      None  -> 缺 seq 或缺 appId：仍尝试发送，但无法对齐结果，不回结果
    """
    seq = cmd.get("seq")
    app_id = cmd.get("appId")
    send_time = cmd.get("sendTime")
    who = str(cmd.get("who") or "").strip()
    text = cmd.get("text")
    at = cmd.get("at") or None
    files = cmd.get("files") or None

    has_seq = seq not in (None, "")
    has_app = bool(str(app_id or "").strip())
    report = has_seq and has_app   # 只有两者齐全才回结果到 result topic
    if not report:
        miss = "/".join(n for n, ok in (("appId", has_app), ("seq", has_seq)) if not ok)
        print(f"  ! 指令缺 {miss}（仍尝试发送，但不回结果）: {cmd!r}", flush=True)

    result = {
        "seq": seq,
        "appId": app_id,
        "bot": _bot_id,
        "who": who,
        "ok": False,
        "skipped": False,          # True = 主动没发（过期 / 校验失败）
        "error": None,
        "via": None,
        "sendTime": send_time,
        "delaySeconds": None,
        "ts": datetime.now().isoformat(timespec="seconds"),
    }

    def _ret():
        return result if report else None

    if not who:
        result["skipped"] = True
        result["error"] = "missing 'who'"
        print(f"  ! 指令缺 who，未发送: {cmd!r}", flush=True)
        return _ret()
    if not text and not files:
        result["skipped"] = True
        result["error"] = "empty: no 'text' or 'files'"
        print(f"  ! 指令无 text/files，未发送: {cmd!r}", flush=True)
        return _ret()

    # ---- 过期检查 ----
    dt = _parse_send_time(send_time)
    if dt is None:
        if send_time not in (None, ""):
            print(f"  ! sendTime 无法解析（跳过过期检查）: {send_time!r}", flush=True)
    else:
        delay = (datetime.now() - dt).total_seconds()
        result["delaySeconds"] = round(delay, 1)
        if delay > max_delay_seconds:
            result["skipped"] = True
            result["error"] = f"stale: delay {int(delay)}s > {max_delay_seconds}s"
            print(f"  ! 指令过期 {int(delay)}s（>{max_delay_seconds}s），不发送: who={who}", flush=True)
            return _ret()

    sub = None
    if who in registered:
        try:
            sub = wx.GetSubWindow(nickname=who)
        except Exception:
            sub = None
    result["via"] = "subwindow" if sub else "mainwindow"

    try:
        if files:
            if sub:
                sub.SendFiles(filepath=files)
            else:
                wx.SendFiles(who=who, filepath=files)
        if text:
            if sub:
                sub.SendMsg(msg=text, at=at) if at else sub.SendMsg(text)
            else:
                wx.SendMsg(msg=text, who=who, at=at) if at else wx.SendMsg(msg=text, who=who)
        result["ok"] = True
        print(f"  → 已发送到 {who}（{result['via']}）: text={text!r} at={at} files={files}", flush=True)
    except Exception as e:
        result["error"] = repr(e)
        print(f"  ! 发送到 {who} 失败: {e!r}", flush=True)
    return _ret()


def main():
    global _sink, _bot_id

    cfg = load_config()
    state_path = STATE_PATH
    print(f"配置文件 {CONFIG_PATH}")

    # 启动：先从状态文件恢复上次的监听名单
    init_listen, init_group = load_state(state_path)
    init_targets = _clean_names(list(init_listen) + list(init_group))
    print(f"状态文件 {state_path}")
    print(f"  恢复 listen_list={init_listen}  group={init_group}"
          if init_targets else "  无历史名单（等待 Redis 快照）")

    wx = connect_wechat()
    _bot_id = wx.nickname
    print(f"已连接微信：{wx.nickname}")

    brokers, topic, send_topic, send_result_topic, group_id = kafka_settings(cfg)
    try:
        max_delay = int(cfg.get("send_max_delay_seconds") or DEFAULTS["send_max_delay_seconds"])
    except (TypeError, ValueError):
        max_delay = DEFAULTS["send_max_delay_seconds"]
    print(f"Kafka -> brokers={brokers}")
    print(f"  入站消息 topic={topic}")
    print(f"  出站指令 topic={send_topic}  group.id={group_id}")
    print(f"  发送结果 topic={send_result_topic}")
    print(f"  指令过期阈值 send_max_delay_seconds={max_delay}")
    _sink = KafkaSink(brokers=brokers, topic=topic)
    _sink.start()

    source = KafkaSource(brokers, send_topic, group_id, on_command=_send_q.put)
    source.start()

    redis_url, redis_channel, redis_password = redis_settings(cfg)
    _pw_hint = "（密码：redis_password）" if redis_password else "（密码：url 内/无）"
    print(f"Redis -> url={_mask_url(redis_url)}  channel={redis_channel} {_pw_hint}")

    def on_snapshot(listen_list, group):
        # 订阅线程回调：把全量快照原样塞进队列，reconcile + 落盘交给主线程
        _target_q.put(("redis", list(listen_list), list(group)))

    control = RedisListenControl(redis_url, redis_channel, on_snapshot, password=redis_password)
    control.start()

    wx.StopListening()
    time.sleep(1)
    wx.StartListening()

    registered = set()
    reconcile(wx, registered, set(init_targets))

    print(f"\n开始监听，当前 {len(registered)} 个对象：{sorted(registered)}")
    print(f"（Redis channel {redis_channel} 发全量快照改监听名单；"
          f"Kafka topic {send_topic} 发指令由本进程 SendMsg；Ctrl+C 退出）\n")

    try:
        while True:
            time.sleep(0.5)

            # 1) Redis 全量快照 -> reconcile 监听名单 + 落盘
            snap = _drain_latest(_target_q)
            if snap is not None:
                _src, listen_list, group = snap
                want = set(_clean_names(list(listen_list) + list(group)))
                print(f"[{datetime.now():%H:%M:%S}] 收到 Redis 全量快照，重新对账监听名单...")
                reconcile(wx, registered, want)
                save_state(state_path, listen_list, group)   # 落盘：下次启动据此恢复
                print(f"  当前监听 {len(registered)} 个对象：{sorted(registered)}\n")

            # 2) Kafka 出站指令 -> SendMsg（清空队列，逐条发，之间轻微节流）
            while True:
                try:
                    cmd = _send_q.get_nowait()
                except queue.Empty:
                    break
                print(f"[{datetime.now():%H:%M:%S}] 收到发送指令: {cmd!r}")
                res = do_send(wx, registered, cmd, max_delay)
                if res is None:
                    continue   # 缺 seq 或 appId：已尝试发送，但不回结果
                _sink.emit(res, topic=send_result_topic)   # 结果回投（含过期/校验失败）
                if not res.get("skipped"):
                    time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n收到退出信号，停止监听...")
    finally:
        for closer in (
            lambda: control.stop(),
            lambda: source.stop(),
            lambda: wx.StopListening(),
            lambda: _sink.stop() if _sink is not None else None,
        ):
            try:
                closer()
            except Exception:
                pass
    return 0


if __name__ == '__main__':
    sys.exit(main())
