#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
白名单监听 Demo
------------------------------------------------------------
独立脚本，复用 wxbot_core 的配置加载器读取 config/config.json 里的
白名单联系人（listen_list）与群（group），用 wxautox4 的回调推送模式
注册监听，收到消息后直接打印，不做任何 AI 回复 / 指令处理 / 转发。

用法：
    python listen_whitelist.py

前置条件与主程序一致：Windows + 已登录并打开的微信 PC 客户端，
wxautox4 已激活，屏幕缩放 100%。
Ctrl+C 退出。
"""

import sys
import time
from datetime import datetime

# 复用主项目的配置加载器（会读取 config/config.json 并同步到属性）
from wxbot_core import WXBotConfig

# 直接使用 wxautox4，不走 WXBot 那套 AI/指令流水线
from wxautox4 import WeChat


def load_whitelist():
    """从配置文件读取白名单联系人与群列表。"""
    cfg = WXBotConfig()          # __init__ 里已自动 load_config + update_global_config
    contacts = list(cfg.listen_list or [])
    groups = list(cfg.group or [])
    if cfg.AllListen_switch:
        print("[提示] 当前 config.json 的 AllListen_switch=True（全局/黑名单模式），"
              "本脚本仍按白名单方式只监听 listen_list + group 中的对象。")
    return contacts, groups


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
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    who = getattr(chat, 'who', '?')
    # attr: friend=别人发来  self=自己发的(多端同步)  system=系统消息
    print(
        f"[{ts}] 会话:{who} | 属性:{msg.attr} | 类型:{msg.type} | "
        f"发送人:{msg.sender}\n    内容: {msg.content}\n",
        flush=True,
    )


def main():
    contacts, groups = load_whitelist()
    targets = contacts + groups
    if not targets:
        print("白名单为空：config.json 的 listen_list 和 group 都没有内容，退出。")
        return 1

    print(f"白名单联系人 {len(contacts)} 个：{contacts}")
    print(f"白名单群 {len(groups)} 个：{groups}")

    wx = connect_wechat()
    print(f"已连接微信：{wx.nickname}")

    wx.StopListening()
    time.sleep(1)
    wx.StartListening()

    ok, fail = [], []
    for name in targets:
        time.sleep(0.5)
        result = wx.AddListenChat(nickname=name, callback=on_message)
        if result:
            ok.append(name)
            print(f"  + 已监听 {name}")
        else:
            fail.append(name)
            msg = result.get('message', result) if isinstance(result, dict) else result
            print(f"  ! 监听失败 {name}: {msg}")

    if not ok:
        print("没有任何对象注册成功，退出。")
        wx.StopListening()
        return 1

    print(f"\n开始监听，共 {len(ok)} 个对象。Ctrl+C 退出。\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n收到退出信号，停止监听...")
    finally:
        try:
            wx.StopListening()
        except Exception:
            pass
    return 0


if __name__ == '__main__':
    sys.exit(main())
