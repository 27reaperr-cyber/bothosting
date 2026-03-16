"""
utils.py — Shared helpers: keyboards, text formatting, state machine.
"""

import re
import time
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# ──────────────────────────────────────────────
# Reply (main) keyboard
# ──────────────────────────────────────────────
def main_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="🤖 Мои боты"),
        KeyboardButton(text="➕ Создать бота"),
    )
    builder.row(
        KeyboardButton(text="📊 Статус системы"),
        KeyboardButton(text="🖥 Консоль"),
    )
    builder.row(
        KeyboardButton(text="⚙ Настройки"),
    )
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=False)


# ──────────────────────────────────────────────
# Bot list keyboard
# ──────────────────────────────────────────────
def bots_list_keyboard(bots: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for bot in bots:
        status_icon = {"running": "🟢", "stopped": "⚪", "error": "🔴"}.get(bot["status"], "⚪")
        builder.row(InlineKeyboardButton(
            text=f"{status_icon} {bot['name']}",
            callback_data=f"bot_open:{bot['id']}"
        ))
    return builder.as_markup()


# ──────────────────────────────────────────────
# Single bot control keyboard
# ──────────────────────────────────────────────
def bot_control_keyboard(bot_id: int, status: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if status != "running":
        builder.button(text="▶ Запустить", callback_data=f"bot_start:{bot_id}")
    else:
        builder.button(text="⏹ Остановить", callback_data=f"bot_stop:{bot_id}")

    builder.button(text="🔄 Перезапустить", callback_data=f"bot_restart:{bot_id}")
    builder.adjust(2)

    builder.row(
        InlineKeyboardButton(text="📜 Логи", callback_data=f"bot_logs:{bot_id}"),
        InlineKeyboardButton(text="✏ ENV", callback_data=f"bot_env:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text="📁 Файлы", callback_data=f"bot_files:{bot_id}"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"bot_delete_confirm:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="bots_list"),
    )
    return builder.as_markup()


def bot_logs_keyboard(bot_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"bot_logs:{bot_id}"),
        InlineKeyboardButton(text="⏹ Остановить", callback_data=f"bot_stop:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text="🔙 К боту", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


def env_keyboard(bot_id: int, env_vars: dict) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for key in list(env_vars.keys())[:10]:
        builder.row(
            InlineKeyboardButton(text=f"✏ {key}", callback_data=f"env_edit:{bot_id}:{key}"),
            InlineKeyboardButton(text=f"🗑", callback_data=f"env_del:{bot_id}:{key}"),
        )
    builder.row(
        InlineKeyboardButton(text="➕ Добавить", callback_data=f"env_add:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text="📋 Редактировать всё", callback_data=f"env_edit_all:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text="🔙 К боту", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


def files_keyboard(bot_id: int, files: list[str]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for fname in files[:15]:
        safe = fname.replace(":", "_")
        builder.row(InlineKeyboardButton(
            text=f"📄 {fname}",
            callback_data=f"file_view:{bot_id}:{safe}"
        ))
    builder.row(
        InlineKeyboardButton(text="🔙 К боту", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


def file_view_keyboard(bot_id: int, filename: str) -> InlineKeyboardMarkup:
    safe = filename.replace(":", "_")
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✏ Редактировать", callback_data=f"file_edit:{bot_id}:{safe}"),
        InlineKeyboardButton(text="🔙 Файлы", callback_data=f"bot_files:{bot_id}"),
    )
    return builder.as_markup()


def confirm_delete_keyboard(bot_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"bot_delete:{bot_id}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


def cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="cancel")
    return builder.as_markup()


def console_keyboard(bot_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"console_refresh:{bot_id}"),
        InlineKeyboardButton(text="⏹ Остановить", callback_data=f"bot_stop:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text="🔙 К боту", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


# ──────────────────────────────────────────────
# Text formatting
# ──────────────────────────────────────────────
def format_bot_card(bot: dict, show_details: bool = True) -> str:
    status_map = {
        "running": "🟢 Работает",
        "stopped": "⚪ Остановлен",
        "error":   "🔴 Ошибка",
        "installing": "⚙️ Установка...",
        "starting": "🔄 Запускается...",
    }
    status_text = status_map.get(bot["status"], bot["status"])
    pid_str = f"\n🔢 PID: {bot['pid']}" if bot.get("pid") and bot["status"] == "running" else ""
    restarts_str = f"\n♻️ Рестартов: {bot['restarts']}" if bot.get("restarts") else ""

    lines = [
        f"📦 <b>{bot['name']}</b>",
        f"",
        f"📊 Статус: {status_text}",
        f"📄 Файл запуска: <code>{bot['main_file']}</code>",
    ]
    if show_details:
        lines.append(pid_str.strip() or "")
        lines.append(restarts_str.strip() or "")

    return "\n".join(l for l in lines if l != "")


def format_welcome(stats: dict, user_name: str) -> str:
    bots_count = stats.get("total_bots", 0)
    active = stats.get("active_bots", 0)
    ram = stats.get("ram_used_mb", 0)
    cpu = stats.get("cpu_percent", 0)

    return (
        f"🚀 <b>Telegram Bot Hosting</b>\n\n"
        f"Привет, <b>{user_name}</b>! 👋\n"
        f"Добро пожаловать в систему управления ботами.\n\n"
        f"📊 Активные боты: <b>{active}</b>\n"
        f"🤖 Всего ботов: <b>{bots_count}</b>\n"
        f"💾 RAM: <b>{ram} MB</b>\n"
        f"🖥 CPU: <b>{cpu}%</b>\n\n"
        f"Выберите действие в меню ниже."
    )


def format_system_status(stats: dict, all_bots: list) -> str:
    running = sum(1 for b in all_bots if b["status"] == "running")
    error = sum(1 for b in all_bots if b["status"] == "error")

    return (
        f"📊 <b>Статус системы</b>\n\n"
        f"🖥 CPU: <b>{stats.get('cpu_percent', 0)}%</b>\n"
        f"💾 RAM: <b>{stats.get('ram_used_mb', 0)} / {stats.get('ram_total_mb', 0)} MB</b>\n\n"
        f"🤖 Ботов всего: <b>{len(all_bots)}</b>\n"
        f"🟢 Запущено: <b>{running}</b>\n"
        f"🔴 Ошибки: <b>{error}</b>\n"
    )


def format_logs(bot_name: str, logs_text: str) -> str:
    # Truncate for Telegram 4096 char limit
    MAX = 3500
    header = f"📟 <b>Консоль</b>\n\n🤖 Бот: <b>{bot_name}</b>\n\n<pre>"
    footer = "</pre>"
    available = MAX - len(header) - len(footer)
    if len(logs_text) > available:
        logs_text = "...\n" + logs_text[-available + 5:]
    return header + _escape_html(logs_text) + footer


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_env(bot_name: str, env_vars: dict) -> str:
    if not env_vars:
        return f"✏ <b>ENV переменные</b>\n\n🤖 <b>{bot_name}</b>\n\n<i>Нет переменных</i>"
    lines = [f"✏ <b>ENV переменные</b>\n\n🤖 <b>{bot_name}</b>\n"]
    for k, v in env_vars.items():
        # Mask sensitive values
        display_v = v if len(v) <= 4 else v[:2] + "***" + v[-2:]
        lines.append(f"<code>{k}</code> = <code>{display_v}</code>")
    return "\n".join(lines)


def format_create_step(step: int, data: dict) -> str:
    steps = {
        1: (
            "➕ <b>Создать бота</b> — Шаг 1/4\n\n"
            "📎 Отправьте <b>ссылку на GitHub репозиторий</b> или прикрепите <b>ZIP архив</b>:\n\n"
            "Пример:\n"
            "<code>https://github.com/user/my-bot</code>"
        ),
        2: (
            f"➕ <b>Создать бота</b> — Шаг 2/4\n\n"
            f"📄 Укажите <b>главный файл запуска</b> бота:\n\n"
            f"Например: <code>main.py</code>, <code>bot.py</code>, <code>app.py</code>"
        ),
        3: (
            f"➕ <b>Создать бота</b> — Шаг 3/4\n\n"
            f"🏷 Введите <b>название</b> для вашего бота:\n\n"
            f"Только буквы, цифры, - и _ (не более 32 символов)"
        ),
        4: (
            f"➕ <b>Создать бота</b> — Шаг 4/4\n\n"
            f"✏ Введите <b>ENV переменные</b> (или нажмите 'Пропустить'):\n\n"
            f"Формат:\n<code>TOKEN=ваш_токен\nAPI_KEY=ваш_ключ</code>"
        ),
    }
    return steps.get(step, "")


def validate_bot_name(name: str) -> tuple[bool, str]:
    name = name.strip()
    if not name:
        return False, "Название не может быть пустым"
    if len(name) > 32:
        return False, "Название слишком длинное (макс 32 символа)"
    if not re.match(r'^[a-zA-Z0-9_\-а-яёА-ЯЁ ]+$', name):
        return False, "Недопустимые символы в названии"
    return True, name


def parse_env_text(text: str) -> dict[str, str]:
    """Parse KEY=VALUE pairs from text."""
    result = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if key:
                result[key] = value
    return result


def is_valid_github_url(url: str) -> bool:
    return bool(re.match(r'https://github\.com/[^/]+/[^/]+', url.strip()))
