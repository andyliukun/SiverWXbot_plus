# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Windows-only WeChat (微信) chatbot framework built on the paid `wxautox4` automation kernel. It drives the WeChat PC client (versions 4.1.9 – 4.1.13.63), routes incoming messages to pluggable AI backends, and is operated through a Flask web panel. Codebase, UI, commit messages, and docs are in Chinese.

## Commands

There is no build system, linter, or committed test suite (`tests/`, `test*.py` are gitignored).

```bash
pip install -r requirements.txt      # deps; needs Python 3.10–3.13 on Windows
python web_server.py                  # the only entry point — starts the panel, auto-opens the browser
```

- `web_server.py` picks a free port in 10001–11000, serves the panel at `http://127.0.0.1:<port>`, and the bot itself is started/stopped from the panel UI (or its `/start_bot` `/stop_bot` routes), not from the CLI.
- `wxautox4` requires a paid activation key or nothing runs (`WXBot.wxautox_activate_check`).
- Release packaging is PyInstaller via `SiverWXbot.spec` (gitignored, not in the repo).
- On release, bump `version` in `wxbot_core.py` **and** `docs/version.json` together — they must match.

## Architecture

### Process model
`web_server.py` (Flask, `threaded=True`) is the long-lived process. Clicking "start" spawns **one** `WXBot` instance in a daemon thread (`start_bot` → `run_bot`), held as the module global `bot`. That thread calls `pythoncom.CoInitialize()` first — any code touching WeChat/COM must run on a COM-initialized thread. `WXBot.main()` is a `while self.run_flag` poll loop (~3s) doing offline detection, new-friend checks, global-listen mode, and `schedule.run_pending()` for timed tasks.

### Modules
- **`wxbot_core.py`** (the engine, ~5k lines). Key classes:
  - `WXBotConfig` — loads/saves the single flat `config/config.json`, merges in defaults, manages the `config/prompt/` directory. `update_global_config()` and `create_new_config_file()` define default keys.
  - `MemoryManager` — per-chat conversation history as JSON under `memory/{wx_id}/{chat_name}/`; storage names are hashed for Windows-reserved/unsafe names.
  - `ReplyCountStore` — per-user reply-round limits in `config/reply_count.json`.
  - AI adapters `OpenAIAPI`, `DifyAPI`, `CozeAPI`, `DusAPI`, `UnconfiguredAPI` — all expose the same `chat(message, model=, stream=, prompt=, history=, ...)` interface. Selected by the `sdk` string on each entry of `config["api_configs"]`; `OPENAI_SDK_ALIASES` covers the OpenAI-compatible path.
  - `WXBot` — orchestrator: builds wxautox listeners (`init_wx_listeners`), receives `message_handle_callback` → `process_message`, which dispatches to admin command (`process_command`), keyword reply, custom-rule forward (`_handle_custom_forward`), or AI reply (`wx_send_ai`). Also runs scheduled messages/moments, moments likes, and new-friend auto-accept.
- **`web_server.py`** — panel: session auth (`config/admin.json`, hashed), config/prompt/memory/email/webhook CRUD routes, backup, `SiverPanelManager` wiring. `_TempAPIConfig` + `/test_api_config` test an interface without saving.
- **`siver_panel.py`** — `SiverPanelManager`, a WebSocket relay client for the optional hosted remote-access service. Loaded dynamically by `web_server.py` (`load_siver_panel_manager_class`); a missing/broken file must not break the panel.
- **`logger.py`** — in-memory ring + file logs in `panel_logs/`. Use `log(level, message)` everywhere in the bot; `log_server(level, msg)` for panel-only events.
- **`email_send.py`** / **`webhook_send.py`** — alert sinks (`config/email.txt`, `config/webhook.json`), fired from the error-notification path.

### Configuration
One flat `config/config.json` (~100 keys). Defaults are defined in **two** places that must stay in sync: `web_server.main()`'s `default_config` dict and `WXBotConfig` in `wxbot_core.py`. When adding a config key, update both plus the dashboard template.

- Prompts live as individual `config/prompt/*.md` files (`默认.md` is the default), not in `config.json`. Bound per conversation via `chat_prompt_map` / `group_prompt_map`.
- Per-group AI interface: `group_api_map` maps a group name to an index into `api_configs` (`_get_group_api` / `_init_api_by_index`, results cached in `api_cache`).
- Two listening modes: whitelist (`listen_list`) vs. global/blacklist (`AllListen_switch`, users in `listen_list` are then excluded).
- Hot reload: the `/更新配置` admin command or a panel save calls `refresh_config()` and re-inits listeners — no process restart for most changes.

### Runtime directories (all gitignored, auto-created)
`config/`, `memory/`, `panel_logs/`, `wxauto_logs/`, `old_wxbot_config/` (startup auto-backup of `config/` and `memory/`).

### Path resolution idiom
Every module has a `_base_dir()` that returns `os.path.dirname(sys.executable)` under PyInstaller onefile (`sys._MEIPASS` present) else `os.path.abspath(".")`. All config/memory/log paths are built from it — follow this pattern for any new file path.

### Admin commands
Sent as WeChat messages to the nickname in `config["admin"]`. `/指令` returns a category menu; each category command lists its details. Dispatch and handlers are `WXBot.process_command` and the `handle_*` methods.
