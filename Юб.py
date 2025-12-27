#!/usr/bin/env python3
# bot.py — Telethon userbot с менеджером модулей в одном файле.
#
# Требования:
#   pip install telethon aiohttp
#
# Переменные окружения:
#   TG_API_ID     — твой API ID
#   TG_API_HASH   — твой API HASH
#   (опционально) GITHUB_TOKEN — для доступа к приватным репозиториям GitHub
#
# Запуск:
#   python bot.py
#
# Внимание: выполнение кода, скачанного из интернета, небезопасно. Устанавливайте только доверенные модули!

import os
import sys
import json
import aiohttp
import asyncio
import importlib
import importlib.util
import traceback
import base64
from typing import Optional, Dict
from datetime import datetime
from telethon import TelegramClient, events

# ----------------------------- Конфигурация ---------------------------------
MODULE_DIR = 'modules'
STATE_FILE = os.path.join(MODULE_DIR, 'installed_modules.json')
SESSION_NAME = 'userbot_session'  # файл сессии будет SESSION_NAME.session
# ---------------------------------------------------------------------------

# Состояние загруженных модулей: name -> {module, path, url, modname, installed_at}
loaded_modules: Dict[str, dict] = {}

if not os.path.exists(MODULE_DIR):
    os.makedirs(MODULE_DIR)


def _save_state():
    data = {}
    for name, entry in loaded_modules.items():
        data[name] = {
            'url': entry.get('url'),
            'path': entry.get('path'),
            'modname': entry.get('modname'),
            'installed_at': entry.get('installed_at'),
        }
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("Failed to save state:", e)


def _load_state_file():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _unique_filename(base: str) -> str:
    name = base
    i = 1
    while os.path.exists(os.path.join(MODULE_DIR, name)):
        name = f"{os.path.splitext(base)[0]}_{i}{os.path.splitext(base)[1]}"
        i += 1
    return name


async def _http_get_text(url: str, headers: Optional[dict] = None) -> str:
    headers = headers or {}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise Exception(f"HTTP {resp.status}: {text[:1000]}")
            return text


async def _fetch_github_api(owner: str, repo: str, path: str, ref: Optional[str] = None) -> str:
    token = os.getenv('GITHUB_TOKEN')
    headers = {}
    if token:
        headers['Authorization'] = f'token {token}'
    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    if ref:
        api_url += f"?ref={ref}"
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, headers=headers) as resp:
            status = resp.status
            text = await resp.text()
            if status >= 400:
                raise Exception(f"GitHub API {status}: {text}")
            try:
                obj = await resp.json()
            except Exception:
                raise Exception("GitHub API: invalid JSON response")
            if isinstance(obj, dict) and obj.get('encoding') == 'base64' and 'content' in obj:
                content_b64 = obj['content']
                return base64.b64decode(content_b64).decode('utf-8', errors='replace')
            raise Exception("Unexpected GitHub API response: not a file or unsupported encoding.")


def _parse_github_blob_url(url: str):
    # https://github.com/{owner}/{repo}/blob/{branch}/{path/to/file.py}
    if 'https://github.com/' not in url:
        return None
    try:
        parts = url.split('https://github.com/')[1].split('/')
        owner = parts[0]
        repo = parts[1]
        if len(parts) < 5 or parts[2] != 'blob':
            return None
        ref = parts[3]
        path = '/'.join(parts[4:])
        return {'owner': owner, 'repo': repo, 'path': path, 'ref': ref}
    except Exception:
        return None


async def download_module_code(url: str) -> (str, str):
    """
    Возвращает (filename, code_text).
    Поддерживает:
      - github.com/.../blob/...  (через GitHub API — подходит для приватных репо с GITHUB_TOKEN)
      - raw.githubusercontent.com/... (публичный)
      - любой прямой raw URL
    """
    gh = _parse_github_blob_url(url)
    if gh:
        code = await _fetch_github_api(gh['owner'], gh['repo'], gh['path'], gh['ref'])
        filename = os.path.basename(gh['path']) or f"module_{abs(hash(url)) & 0xffffffff}.py"
        if not filename.endswith('.py'):
            filename += '.py'
        return filename, code

    if 'raw.githubusercontent.com' in url:
        text = await _http_get_text(url)
        filename = os.path.basename(url.split('?')[0]) or f"module_{abs(hash(url)) & 0xffffffff}.py"
        if not filename.endswith('.py'):
            filename += '.py'
        return filename, text

    # Пробуем просто GET
    text = await _http_get_text(url)
    filename = os.path.basename(url.split('?')[0]) or f"module_{abs(hash(url)) & 0xffffffff}.py"
    if not filename.endswith('.py'):
        filename += '.py'
    return filename, text


def import_module_from_file(modname: str, path: str):
    spec = importlib.util.spec_from_file_location(modname, path)
    if spec is None:
        raise ImportError("Cannot build module spec")
    module = importlib.util.module_from_spec(spec)
    loader = spec.loader
    if loader is None:
        raise ImportError("Cannot load module spec")
    loader.exec_module(module)
    sys.modules[modname] = module
    return module


async def maybe_call(func, client):
    if asyncio.iscoroutinefunction(func):
        await func(client)
    else:
        func(client)


async def _install_from_url(client, url: str):
    filename, code = await download_module_code(url)
    filename = _unique_filename(filename)
    path = os.path.join(MODULE_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(code)
    name = os.path.splitext(filename)[0]
    modname = f"modules.{name}"
    i = 1
    orig_modname = modname
    while modname in sys.modules:
        modname = f"{orig_modname}_{i}"
        i += 1
    module = import_module_from_file(modname, path)
    if hasattr(module, 'setup'):
        await maybe_call(module.setup, client)
    entry = {
        'module': module,
        'path': path,
        'url': url,
        'modname': modname,
        'installed_at': datetime.utcnow().isoformat() + 'Z'
    }
    loaded_modules[name] = entry
    _save_state()
    return name


async def _remove_module(client, name: str):
    entry = loaded_modules.get(name)
    if not entry:
        raise KeyError("not found")
    module = entry['module']
    if hasattr(module, 'teardown'):
        await maybe_call(module.teardown, client)
    modname = entry.get('modname')
    if modname and modname in sys.modules:
        try:
            del sys.modules[modname]
        except KeyError:
            pass
    try:
        os.remove(entry['path'])
    except Exception:
        pass
    del loaded_modules[name]
    _save_state()


async def _reload_module(client, name: str):
    entry = loaded_modules.get(name)
    if not entry:
        raise KeyError("not found")
    url = entry.get('url')
    if not url:
        raise Exception("No URL stored for module (cannot re-download).")
    filename, code = await download_module_code(url)
    path = entry['path']
    with open(path, 'w', encoding='utf-8') as f:
        f.write(code)
    oldmod = entry['module']
    if hasattr(oldmod, 'teardown'):
        await maybe_call(oldmod.teardown, client)
    modname = entry['modname']
    if modname in sys.modules:
        try:
            newmod = importlib.reload(sys.modules[modname])
        except Exception:
            newmod = import_module_from_file(modname, path)
    else:
        newmod = import_module_from_file(modname, path)
    if hasattr(newmod, 'setup'):
        await maybe_call(newmod.setup, client)
    entry['module'] = newmod
    _save_state()


async def _load_saved_modules(client):
    state = _load_state_file()
    for name, info in state.items():
        url = info.get('url')
        path = info.get('path')
        modname = info.get('modname') or f"modules.{name}"
        try:
            if not os.path.exists(path) and url:
                filename, code = await download_module_code(url)
                path = os.path.join(MODULE_DIR, os.path.basename(info.get('path') or filename))
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(code)
            module = import_module_from_file(modname, path)
            if hasattr(module, 'setup'):
                await maybe_call(module.setup, client)
            loaded_modules[name] = {
                'module': module,
                'path': path,
                'url': url,
                'modname': modname,
                'installed_at': info.get('installed_at')
            }
            print(f"Loaded saved module: {name}")
        except Exception as e:
            print(f"Failed to load saved module {name}: {e}")


# ------------------ Telethon command handler (userbot) -----------------------
async def module_command_handler(event):
    # Команды ловим только исходящие (от тебя) — регистрация ниже делает этом filter.
    text = (event.message.message or "").strip()
    parts = text.split(None, 2)
    if len(parts) == 1:
        await event.reply("Использование: .module install <url> | remove <name> | reload <name> | list | info <name>")
        return
    cmd = parts[1].lower()
    arg = parts[2].strip() if len(parts) >= 3 else ''
    try:
        if cmd == 'install':
            if not arg:
                return await event.reply("Нужен URL (raw или https://github.com/.../blob/...). Для приватных репо установите GITHUB_TOKEN.")
            msg = await event.reply("Скачиваю и устанавливаю модуль...")
            name = await _install_from_url(event.client, arg)
            await msg.edit(f"Модуль '{name}' установлен и загружен.")
            return

        if cmd == 'remove':
            if not arg:
                return await event.reply("Укажите имя модуля для удаления (имя файла без .py).")
            msg = await event.reply(f"Удаляю модуль {arg}...")
            await _remove_module(event.client, arg)
            await msg.edit(f"Модуль '{arg}' удалён.")
            return

        if cmd == 'reload':
            if not arg:
                return await event.reply("Укажите имя модуля для перезагрузки.")
            msg = await event.reply(f"Перезагружаю модуль {arg}...")
            await _reload_module(event.client, arg)
            await msg.edit(f"Модуль '{arg}' перезагружен.")
            return

        if cmd == 'list':
            if not loaded_modules:
                return await event.reply("Нет загруженных модулей.")
            lines = []
            for name, info in loaded_modules.items():
                lines.append(f"- {name} (file: {os.path.basename(info['path'])}, url: {info.get('url')})")
            await event.reply("Загруженные модули:\n" + "\n".join(lines))
            return

        if cmd == 'info':
            if not arg:
                return await event.reply("Укажите имя модуля.")
            entry = loaded_modules.get(arg)
            if not entry:
                return await event.reply("Модуль не найден.")
            info_text = (
                f"Name: {arg}\n"
                f"File: {entry.get('path')}\n"
                f"Module name: {entry.get('modname')}\n"
                f"URL: {entry.get('url')}\n"
                f"Installed at: {entry.get('installed_at')}\n"
            )
            await event.reply(info_text)
            return

        await event.reply("Неизвестная команда. Использование: .module install <url> | remove <name> | reload <name> | list | info <name>")
    except KeyError:
        await event.reply("Модуль не найден.")
    except Exception as e:
        tb = traceback.format_exc()
        await event.reply(f"Ошибка: {e}\n{tb[:1500]}")


# ----------------------------- Entry point ----------------------------------
def main():
    api_id = os.getenv('21367522')
    api_hash = os.getenv('ffcea3a0f0d1f63daf33335936570e21')
    if not api_id or not api_hash:
        print("Укажи TG_API_ID и TG_API_HASH в окружении.")
        sys.exit(1)
    try:
        api_id = int(api_id)
    except Exception:
        print("TG_API_ID должен быть числом.")
        sys.exit(1)

    client = TelegramClient(SESSION_NAME, api_id, api_hash)

    # Регистрируем handler команд — outgoing=True чтобы это был userbot (в чатах и личке)
    client.add_event_handler(module_command_handler, events.NewMessage(outgoing=True, pattern=r'^\.module(?:\b| )'))

    async def _on_start():
        print("Client started, loading saved modules ...")
        await _load_saved_modules(client)
        print("Ready. Используй .module install <url> ...")

    with client:
        client.loop.run_until_complete(_on_start())
        try:
            client.run_until_disconnected()
        except KeyboardInterrupt:
            print("Stopping...")

if __name__ == '__main__':
    main()