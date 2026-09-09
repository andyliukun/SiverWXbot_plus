#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把数据异步投递到 Kafka（一个 Producer 线程，可发多个 topic）。

用法：
    from kafka_sink import KafkaSink
    sink = KafkaSink(brokers="localhost:9092", topic="wx-messages")
    sink.start()
    ...
    sink.emit({...})                          # 发到默认 topic
    sink.emit({...}, topic="other.topic")     # 发到指定 topic
    ...
    sink.stop()                               # 退出前调用，flush 未发送的消息

emit 非阻塞、不抛异常，可在 wxautox 回调线程里安全调用。

依赖：pip install confluent-kafka
"""

import json
import queue
import threading

try:
    from confluent_kafka import Producer
except ImportError:  # 允许没装 kafka 库时脚本仍能跑（emit 变成打印）
    Producer = None


class KafkaSink:
    def __init__(self, brokers, topic, *, queue_size=10000, extra_conf=None):
        self.topic = topic
        self._q = queue.Queue(maxsize=queue_size)
        self._stop = threading.Event()
        self._thread = None
        self._dropped = 0
        self._sent = 0
        self._failed = 0
        self._conf = {
            "bootstrap.servers": brokers,
            "enable.idempotence": True,      # 幂等，配合下游按 id 去重
            "acks": "all",
            "linger.ms": 50,                 # 小批量聚合，降开销
            "compression.type": "lz4",
            "message.timeout.ms": 120000,    # 120s 内投递不成功则回调 error
        }
        if extra_conf:
            self._conf.update(extra_conf)

    # ---- 生产侧：在 wxautox 回调线程里调用，必须非阻塞、不抛异常 ----
    def emit(self, payload: dict, *, topic: str = None, key: str = None):
        if key is None:
            key = str(payload.get("chat") or payload.get("who") or payload.get("appId") or "")
        try:
            self._q.put_nowait((topic or self.topic, key, payload))
        except queue.Full:
            self._dropped += 1
            if self._dropped % 100 == 1:
                print(f"[KafkaSink] 队列已满，已丢弃 {self._dropped} 条", flush=True)

    # ---- 消费侧：独占 Producer 的专职线程 ----
    def start(self):
        if Producer is None:
            print("[KafkaSink] 未安装 confluent-kafka，emit 将只打印不投递", flush=True)
        self._thread = threading.Thread(target=self._run, name="KafkaSink", daemon=False)
        self._thread.start()

    def _run(self):
        producer = Producer(self._conf) if Producer is not None else None
        while not self._stop.is_set() or not self._q.empty():
            try:
                topic, key, payload = self._q.get(timeout=0.5)
            except queue.Empty:
                if producer is not None:
                    producer.poll(0)
                continue

            if producer is None:
                print(f"[KafkaSink:dryrun] {topic} <- {payload}", flush=True)
                continue

            value = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            key_b = key.encode("utf-8")
            try:
                producer.produce(topic, key=key_b, value=value, on_delivery=self._on_delivery)
            except BufferError:
                # 本地队列满：给 librdkafka 时间外发后重试一次，再不行就丢
                producer.poll(1)
                try:
                    producer.produce(topic, key=key_b, value=value, on_delivery=self._on_delivery)
                except BufferError:
                    self._dropped += 1
                    print("[KafkaSink] 本地缓冲满，丢弃 1 条", flush=True)
            except Exception as e:
                self._failed += 1
                print(f"[KafkaSink] produce 异常: {e!r}", flush=True)

            producer.poll(0)

        if producer is not None:
            remaining = producer.flush(30)
            if remaining:
                print(f"[KafkaSink] 退出时仍有 {remaining} 条未确认", flush=True)
        print(f"[KafkaSink] 结束。已发送 {self._sent}，失败 {self._failed}，丢弃 {self._dropped}", flush=True)

    def _on_delivery(self, err, msg):
        if err is not None:
            self._failed += 1
            print(f"[KafkaSink] 投递失败: {err}", flush=True)
        else:
            self._sent += 1

    def stop(self, timeout=35):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
