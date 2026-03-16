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
# Custom symbols (replacing emoji)
# ──────────────────────────────────────────────
SYM_BOT         = "⬡"   # бот
SYM_STATS       = "◈"   # статистика
SYM_CONSOLE     = "⌨"   # консоль
SYM_SETTINGS    = "✹"   # настройки
SYM_PLUS        = "✚"   # создать/добавить
SYM_CHECK       = "✔"   # успех
SYM_CROSS       = "✕"   # ошибка/неудача
SYM_WAIT        = "⋯"   # ожидание
SYM_LOCK        = "☒"   # запрет/блокировка
SYM_USER        = "☻"   # пользователь
SYM_ADMIN       = "♔"   # администратор
SYM_DELETE      = "⌫"   # удалить
SYM_FOLDER      = "🗀"   # папка
SYM_FILE        = "🗋"   # файл
SYM_EDIT        = "✎"   # редактировать
SYM_REFRESH     = "↻"   # обновить/перезапуск
SYM_PACKAGE     = "▣"   # пакет/бот (для сообщений)
SYM_WARNING     = "☡"   # предупреждение
SYM_DISK        = "⏺"   # диск/память
SYM_CPU         = "⎔"   # процессор
SYM_LAUNCH      = "⇧"   # запуск/лимит
SYM_STOP        = "■"   # стоп
SYM_LIST        = "≡"   # список
SYM_DOWNLOAD    = "⇩"   # скачивание
SYM_BACK        = "←"   # назад
SYM_RUNNING     = "✔"   # работает (можно использовать SYM_CHECK)
SYM_STOPPED     = "■"   # остановлен (SYM_STOP)
SYM_ERROR       = "✕"   # ошибка (SYM_CROSS)
SYM_INSTALLING  = "⋯"   # установка (SYM_WAIT)
SYM_STARTING    = "↻"   # запускается (SYM_REFRESH)

# ──────────────────────────────────────────────
# Reply (main) keyboard
# ──────────────────────────────────────────────
def main_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text=f"{SYM_BOT} Мои боты"),
        KeyboardButton(text=f"{SYM_PLUS} Создать бота"),
    )
    builder.row(
        KeyboardButton(text=f"{SYM_STATS} Статус системы"),
        KeyboardButton(text=f"{SYM_CONSOLE} Консоль"),
    )
    builder.row(
        KeyboardButton(text=f"{SYM_SETTINGS} Настройки"),
    )
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=False)


# ──────────────────────────────────────────────
# Bot list keyboard
# ──────────────────────────────────────────────
def bots_list_keyboard(bots: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for bot in bots:
        status_icon = {
            "running": SYM_RUNNING,
            "stopped": SYM_STOPPED,
            "error": SYM_ERROR,
        }.get(bot["status"], SYM_WAIT)
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
        builder.button(text=f"{SYM_LAUNCH} Запустить", callback_data=f"bot_start:{bot_id}")
    else:
        builder.button(text=f"{SYM_STOP} Остановить", callback_data=f"bot_stop:{bot_id}")

    builder.button(text=f"{SYM_REFRESH} Перезапустить", callback_data=f"bot_restart:{bot_id}")
    builder.adjust(2)

    builder.row(
        InlineKeyboardButton(text=f"{SYM_FILE} Логи", callback_data=f"bot_logs:{bot_id}"),
        InlineKeyboardButton(text=f"{SYM_EDIT} ENV", callback_data=f"bot_env:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text=f"{SYM_FOLDER} Файлы", callback_data=f"bot_files:{bot_id}"),
        InlineKeyboardButton(text=f"{SYM_DELETE} Удалить", callback_data=f"bot_delete_confirm:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text=f"{SYM_BACK} Назад", callback_data="bots_list"),
    )
    return builder.as_markup()


def bot_logs_keyboard(bot_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=f"{SYM_REFRESH} Обновить", callback_data=f"bot_logs:{bot_id}"),
        InlineKeyboardButton(text=f"{SYM_STOP} Остановить", callback_data=f"bot_stop:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text=f"{SYM_BACK} К боту", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


def env_keyboard(bot_id: int, env_vars: dict) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for key in list(env_vars.keys())[:10]:
        builder.row(
            InlineKeyboardButton(text=f"{SYM_EDIT} {key}", callback_data=f"env_edit:{bot_id}:{key}"),
            InlineKeyboardButton(text=f"{SYM_DELETE}", callback_data=f"env_del:{bot_id}:{key}"),
        )
    builder.row(
        InlineKeyboardButton(text=f"{SYM_PLUS} Добавить", callback_data=f"env_add:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text=f"{SYM_LIST} Редактировать всё", callback_data=f"env_edit_all:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text=f"{SYM_BACK} К боту", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


def files_keyboard(bot_id: int, files: list[str]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for fname in files[:15]:
        safe = fname.replace(":", "_")
        builder.row(InlineKeyboardButton(
            text=f"{SYM_FILE} {fname}",
            callback_data=f"file_view:{bot_id}:{safe}"
        ))
    builder.row(
        InlineKeyboardButton(text=f"{SYM_BACK} К боту", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


def file_view_keyboard(bot_id: int, filename: str) -> InlineKeyboardMarkup:
    safe = filename.replace(":", "_")
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=f"{SYM_EDIT} Редактировать", callback_data=f"file_edit:{bot_id}:{safe}"),
        InlineKeyboardButton(text=f"{SYM_BACK} Файлы", callback_data=f"bot_files:{bot_id}"),
    )
    return builder.as_markup()


def confirm_delete_keyboard(bot_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=f"{SYM_CHECK} Да, удалить", callback_data=f"bot_delete:{bot_id}"),
        InlineKeyboardButton(text=f"{SYM_CROSS} Отмена", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


def cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=f"{SYM_CROSS} Отмена", callback_data="cancel")
    return builder.as_markup()


def console_keyboard(bot_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=f"{SYM_REFRESH} Обновить", callback_data=f"console_refresh:{bot_id}"),
        InlineKeyboardButton(text=f"{SYM_STOP} Остановить", callback_data=f"bot_stop:{bot_id}"),
    )
    builder.row(
        InlineKeyboardButton(text=f"{SYM_BACK} К боту", callback_data=f"bot_open:{bot_id}"),
    )
    return builder.as_markup()


# ──────────────────────────────────────────────
# Text formatting
# ──────────────────────────────────────────────
def format_bot_card(bot: dict, show_details: bool = True) -> str:
    status_map = {
        "running":    f"{SYM_RUNNING} Работает",
        "stopped":    f"{SYM_STOPPED} Остановлен",
        "error":      f"{SYM_ERROR} Ошибка",
        "installing": f"{SYM_INSTALLING} Установка...",
        "starting":   f"{SYM_STARTING} Запускается...",
    }
    status_text = status_map.get(bot["status"], bot["status"])
    pid_str = f"\n{SYM_STATS} PID: {bot['pid']}" if bot.get("pid") and bot["status"] == "running" else ""
    restarts_str = f"\n{SYM_REFRESH} Рестартов: {bot['restarts']}" if bot.get("restarts") else ""

    lines = [
        f"{SYM_PACKAGE} <b>{bot['name']}</b>",
        f"",
        f"{SYM_STATS} Статус: {status_text}",
        f"{SYM_FILE} Файл запуска: <code>{bot['main_file']}</code>",
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
        f"{SYM_LAUNCH} <b>Telegram Bot Hosting</b>\n\n"
        f"Привет, <b>{user_name}</b>! {SYM_USER}\n"
        f"Добро пожаловать в систему управления ботами.\n\n"
        f"{SYM_STATS} Активные боты: <b>{active}</b>\n"
        f"{SYM_BOT} Всего ботов: <b>{bots_count}</b>\n"
        f"{SYM_DISK} RAM: <b>{ram} MB</b>\n"
        f"{SYM_CPU} CPU: <b>{cpu}%</b>\n\n"
        f"Выберите действие в меню ниже."
    )


def format_system_status(stats: dict, all_bots: list) -> str:
    running = sum(1 for b in all_bots if b["status"] == "running")
    error = sum(1 for b in all_bots if b["status"] == "error")

    return (
        f"{SYM_STATS} <b>Статус системы</b>\n\n"
        f"{SYM_CPU} CPU: <b>{stats.get('cpu_percent', 0)}%</b>\n"
        f"{SYM_DISK} RAM: <b>{stats.get('ram_used_mb', 0)} / {stats.get('ram_total_mb', 0)} MB</b>\n\n"
        f"{SYM_BOT} Ботов всего: <b>{len(all_bots)}</b>\n"
        f"{SYM_RUNNING} Запущено: <b>{running}</b>\n"
        f"{SYM_ERROR} Ошибки: <b>{error}</b>\n"
    )


def format_logs(bot_name: str, logs_text: str) -> str:
    # Truncate for Telegram 4096 char limit
    MAX = 3500
    header = f"{SYM_CONSOLE} <b>Консоль</b>\n\n{SYM_BOT} Бот: <b>{bot_name}</b>\n\n<pre>"
    footer = "</pre>"
    available = MAX - len(header) - len(footer)
    if len(logs_text) > available:
        logs_text = "...\n" + logs_text[-available + 5:]
    return header + _escape_html(logs_text) + footer


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_env(bot_name: str, env_vars: dict) -> str:
    if not env_vars:
        return f"{SYM_EDIT} <b>ENV переменные</b>\n\n{SYM_BOT} <b>{bot_name}</b>\n\n<i>Нет переменных</i>"
    lines = [f"{SYM_EDIT} <b>ENV переменные</b>\n\n{SYM_BOT} <b>{bot_name}</b>\n"]
    for k, v in env_vars.items():
        # Mask sensitive values
        display_v = v if len(v) <= 4 else v[:2] + "***" + v[-2:]
        lines.append(f"<code>{k}</code> = <code>{display_v}</code>")
    return "\n".join(lines)


def format_create_step(step: int, data: dict) -> str:
    steps = {
        1: (
            f"{SYM_PLUS} <b>Создать бота</b> — Шаг 1/4\n\n"
            f"{SYM_FILE} Отправьте <b>ссылку на GitHub репозиторий</b> или прикрепите <b>ZIP архив</b>:\n\n"
            "Пример:\n"
            "<code>https://github.com/user/my-bot</code>"
        ),
        2: (
            f"{SYM_PLUS} <b>Создать бота</b> — Шаг 2/4\n\n"
            f"{SYM_FILE} Укажите <b>главный файл запуска</b> бота:\n\n"
            f"Например: <code>main.py</code>, <code>bot.py</code>, <code>app.py</code>"
        ),
        3: (
            f"{SYM_PLUS} <b>Создать бота</b> — Шаг 3/4\n\n"
            f"{SYM_BOT} Введите <b>название</b> для вашего бота:\n\n"
            f"Только буквы, цифры, - и _ (не более 32 символов)"
        ),
        4: (
            f"{SYM_PLUS} <b>Создать бота</b> — Шаг 4/4\n\n"
            f"{SYM_EDIT} Введите <b>ENV переменные</b> (или нажмите 'Пропустить'):\n\n"
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