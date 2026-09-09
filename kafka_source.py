#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
消费 Kafka 出站 topic 的「发送指令」，回调交给主线程执行 chat.SendMsg。

指令格式（JSON，一条一个）：
    {"appId": "crm", "id": "req-123", "who": "张三", "text": "你好"}
    {"appId": "crm", "who": "项目群", "text": "通知一下", "at": ["李四", "王五"]}
    {"appId": "crm", "who": "张三", "files": ["D:/a.png", "D:/b.pdf"]}
  appId 必填，上游调用方标识（会原样回传到 result topic）
  id    可选，调用方自定义的相关性 id（回传，用于对齐请求与结果）
  who   必填，联系人或群名（需与微信里一致）
  text  可选，文本
  at    可选，str 或 list，仅群聊有效
  files 可选，list，图片/文件/视频路径

发送结果会投递到 <topic>.result（见 listen_whitelist.py）。

用法：
    from kafka_source import KafkaSource
    src = KafkaSource(brokers, topic, group_id, on_command=lambda cmd: q.put(cmd))
    src.start()
    ...
    src.stop()

on_command(cmd: dict) 在消费线程里执行，别在里面直接调 wxautox；
把 cmd 丢进队列，由持有 WeChat 对象的主线程消费并 SendMsg。

从最新位点开始消费（auto.offset.reset=latest），避免重启后重放历史指令
造成重复发送。

依赖：pip install confluent-kafka
"""

import json
import threading

try:
    from confluent_kafka import Consumer, KafkaException
except ImportError:
    Consumer = None
    KafkaException = Exception


class KafkaSource:
    def __init__(self, brokers, topic, group_id, on_command, *, extra_conf=None):
        self.topic = topic
        self.on_command = on_command
        self._stop = threading.Event()
        self._thread = None
        self._conf = {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "latest",   # 只消费新指令，不重放历史
            "enable.auto.commit": True,
            "socket.keepalive.enable": True,
        }
        if extra_conf:
            self._conf.update(extra_conf)

    def start(self):
        if Consumer is None:
            print("[KafkaSource] 未安装 confluent-kafka，出站指令消费不可用", flush=True)
            return
        self._thread = threading.Thread(target=self._run, name="KafkaSource", daemon=True)
        self._thread.start()

    def stop(self, timeout=10):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    # ---------------------------------------------------------

    def _run(self):
        consumer = Consumer(self._conf)
        try:
            consumer.subscribe([self.topic])
            print(f"[KafkaSource] 已订阅 topic: {self.topic}", flush=True)
            while not self._stop.is_set():
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"[KafkaSource] poll 错误: {msg.error()}", flush=True)
                    continue
                self._handle(msg.value())
        except Exception as e:
            print(f"[KafkaSource] 消费循环异常退出: {e!r}", flush=True)
        finally:
            try:
                consumer.close()
            except Exception:
                pass
            print("[KafkaSource] 已停止", flush=True)

    def _handle(self, raw):
        try:
            cmd = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except Exception as e:
            print(f"[KafkaSource] 指令解析失败: {e!r}  raw={raw!r}", flush=True)
            return
        if not isinstance(cmd, dict):
            print(f"[KafkaSource] 忽略非对象指令: {cmd!r}", flush=True)
            return
        # 字段校验交给主线程 do_send，那里能把校验失败也发到 result topic
        try:
            self.on_command(cmd)
        except Exception as e:
            print(f"[KafkaSource] on_command 回调出错: {e!r}", flush=True)
