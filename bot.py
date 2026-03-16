"""
bot.py — Main Telegram Bot Hosting interface.
Handles all user interactions via aiogram 3.x.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, Document, Message,
    InlineKeyboardButton, InlineKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

import db
import runner
import utils

load_dotenv()

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/bot_host.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger("bothost.main")

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = set(int(x) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit())
MAX_BOTS_PER_USER = int(os.getenv("MAX_BOTS_PER_USER", "5"))

if not BOT_TOKEN:
    sys.exit("❌ BOT_TOKEN is not set in .env")

Path("data").mkdir(exist_ok=True)
Path("bots").mkdir(exist_ok=True)
Path("tmp").mkdir(exist_ok=True)

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

# ──────────────────────────────────────────────
# FSM States
# ──────────────────────────────────────────────
class CreateBot(StatesGroup):
    waiting_source   = State()   # GitHub URL or ZIP
    waiting_mainfile = State()   # main.py
    waiting_name     = State()   # bot name
    waiting_env      = State()   # env vars


class EditEnv(StatesGroup):
    waiting_key      = State()
    waiting_value    = State()
    waiting_all_env  = State()


class EditFile(StatesGroup):
    waiting_content  = State()


# ──────────────────────────────────────────────
# Middleware: rate limit + user registration
# ──────────────────────────────────────────────
from aiogram import BaseMiddleware
from typing import Callable, Any


class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable, event: Any, data: dict) -> Any:
        user = data.get("event_from_user")
        if user:
            db.upsert_user(user.id, user.username or "", user.first_name or "")
            if db.is_banned(user.id):
                if hasattr(event, "answer"):
                    await event.answer(f"{SYM_LOCK} Вы заблокированы в системе.")
                return
        return await handler(event, data)


class RateLimitMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable, event: Any, data: dict) -> Any:
        user = data.get("event_from_user")
        if user and not user.id in ADMIN_IDS:
            if not db.check_rate_limit(user.id, "action"):
                msg = event if isinstance(event, Message) else getattr(event, "message", None)
                if msg:
                    await msg.answer(
                        f"{SYM_WAIT} <b>Слишком много запросов!</b>\n\n"
                        "Подождите немного и попробуйте снова.\n"
                        "Лимит: 10 действий в минуту.",
                        parse_mode=ParseMode.HTML
                    )
                return
        return await handler(event, data)


# ──────────────────────────────────────────────
# Router
# ──────────────────────────────────────────────
router = Router()


# ── /start ────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(msg: Message):
    user_id = msg.from_user.id
    user = db.get_user(user_id)
    bots = db.get_user_bots(user_id)
    stats = runner.get_system_stats()
    stats["total_bots"] = len(bots)

    await msg.answer(
        utils.format_welcome(stats, msg.from_user.first_name or "друг"),
        reply_markup=utils.main_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# ── Мои боты ───────────────────────────────
@router.message(F.text == f"{SYM_BOT} Мои боты")
async def show_my_bots(msg: Message):
    user_id = msg.from_user.id
    bots = db.get_user_bots(user_id)
    if not bots:
        await msg.answer(
            f"{SYM_BOT} <b>Мои боты</b>\n\n"
            "У вас пока нет ботов.\n"
            f"Нажмите <b>{SYM_PLUS} Создать бота</b>, чтобы добавить первого!",
            parse_mode=ParseMode.HTML,
            reply_markup=utils.main_keyboard(),
        )
        return

    await msg.answer(
        f"{SYM_BOT} <b>Мои боты</b> ({len(bots)})\n\n"
        "Выберите бота для управления:",
        reply_markup=utils.bots_list_keyboard(bots),
        parse_mode=ParseMode.HTML,
    )


# ── Статус системы ─────────────────────────
@router.message(F.text == f"{SYM_STATS} Статус системы")
async def show_status(msg: Message):
    stats = runner.get_system_stats()
    all_bots = db.get_user_bots(msg.from_user.id)
    await msg.answer(
        utils.format_system_status(stats, all_bots),
        parse_mode=ParseMode.HTML,
        reply_markup=utils.main_keyboard(),
    )


# ── Консоль ────────────────────────────────
@router.message(F.text == f"{SYM_CONSOLE} Консоль")
async def show_console(msg: Message):
    user_id = msg.from_user.id
    bots = db.get_user_bots(user_id)
    running = [b for b in bots if b["status"] == "running"]
    if not running:
        await msg.answer(
            f"{SYM_CONSOLE} <b>Консоль</b>\n\nНет запущенных ботов.",
            parse_mode=ParseMode.HTML
        )
        return

    # Show console for first running bot
    bot_data = running[0]
    logs = runner.get_logs(user_id, bot_data["id"])
    await msg.answer(
        utils.format_logs(bot_data["name"], logs),
        reply_markup=utils.console_keyboard(bot_data["id"]),
        parse_mode=ParseMode.HTML,
    )


# ── Настройки ──────────────────────────────
@router.message(F.text == f"{SYM_SETTINGS} Настройки")
async def show_settings(msg: Message):
    user_id = msg.from_user.id
    user = db.get_user(user_id)
    bots = db.get_user_bots(user_id)
    max_bots = user.get("max_bots", MAX_BOTS_PER_USER) if user else MAX_BOTS_PER_USER

    text = (
        f"{SYM_SETTINGS} <b>Настройки</b>\n\n"
        f"{SYM_USER} ID: <code>{user_id}</code>\n"
        f"{SYM_BOT} Ботов: {len(bots)} / {max_bots}\n"
        f"{SYM_STATS} Лимит действий: 10/мин\n"
        f"{SYM_LAUNCH} Лимит запусков: 3/мин\n"
    )
    if user_id in ADMIN_IDS:
        text += f"\n{SYM_ADMIN} <b>Режим администратора</b>"

    await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=utils.main_keyboard())


# ──────────────────────────────────────────────
# Callback: open bot
# ──────────────────────────────────────────────
@router.callback_query(F.data.startswith("bot_open:"))
async def cb_bot_open(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS} Бот не найден", show_alert=True)
        return

    await cb.message.edit_text(
        utils.format_bot_card(bot_data),
        reply_markup=utils.bot_control_keyboard(bot_id, bot_data["status"]),
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


@router.callback_query(F.data == "bots_list")
async def cb_bots_list(cb: CallbackQuery):
    bots = db.get_user_bots(cb.from_user.id)
    if not bots:
        await cb.message.edit_text(f"{SYM_BOT} Список ботов пуст.")
        return
    await cb.message.edit_text(
        f"{SYM_BOT} <b>Мои боты</b> ({len(bots)})\n\nВыберите бота:",
        reply_markup=utils.bots_list_keyboard(bots),
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


# ──────────────────────────────────────────────
# Callback: Start / Stop / Restart
# ──────────────────────────────────────────────
@router.callback_query(F.data.startswith("bot_start:"))
async def cb_bot_start(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS} Доступ запрещён", show_alert=True)
        return

    if not db.check_rate_limit(cb.from_user.id, "bot_start"):
        await cb.answer(f"{SYM_WAIT} Лимит запусков: 3/мин. Подождите.", show_alert=True)
        return

    await cb.message.edit_text(
        f"{SYM_PACKAGE} <b>{bot_data['name']}</b>\n\n{SYM_WAIT} Запускается...",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()

    asyncio.create_task(_start_bot_task(cb, bot_data, bot_id))


async def _start_bot_task(cb: CallbackQuery, bot_data: dict, bot_id: int):
    user_id = cb.from_user.id
    ok, msg = await runner.start_bot(user_id, bot_id)
    bot_data = db.get_bot(bot_id)

    status_msg = f"{SYM_CHECK} {msg}" if ok else f"{SYM_CROSS} {msg}"
    text = utils.format_bot_card(bot_data) + f"\n\n{status_msg}"
    try:
        await cb.message.edit_text(
            text,
            reply_markup=utils.bot_control_keyboard(bot_id, bot_data["status"]),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("bot_stop:"))
async def cb_bot_stop(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS} Доступ запрещён", show_alert=True)
        return

    await cb.answer(f"{SYM_STOP} Останавливаем...")
    ok, msg = await runner.stop_bot(bot_id)
    bot_data = db.get_bot(bot_id)

    status_msg = f"{SYM_CHECK} {msg}" if ok else f"{SYM_CROSS} {msg}"
    try:
        await cb.message.edit_text(
            utils.format_bot_card(bot_data) + f"\n\n{status_msg}",
            reply_markup=utils.bot_control_keyboard(bot_id, bot_data["status"]),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("bot_restart:"))
async def cb_bot_restart(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS} Доступ запрещён", show_alert=True)
        return

    if not db.check_rate_limit(cb.from_user.id, "bot_start"):
        await cb.answer(f"{SYM_WAIT} Лимит. Подождите.", show_alert=True)
        return

    await cb.message.edit_text(
        f"{SYM_PACKAGE} <b>{bot_data['name']}</b>\n\n{SYM_REFRESH} Перезапускается...",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()
    asyncio.create_task(_restart_bot_task(cb, bot_id))


async def _restart_bot_task(cb: CallbackQuery, bot_id: int):
    user_id = cb.from_user.id
    ok, msg = await runner.restart_bot(user_id, bot_id)
    bot_data = db.get_bot(bot_id)
    status_msg = f"{SYM_CHECK} {msg}" if ok else f"{SYM_CROSS} {msg}"
    try:
        await cb.message.edit_text(
            utils.format_bot_card(bot_data) + f"\n\n{status_msg}",
            reply_markup=utils.bot_control_keyboard(bot_id, bot_data["status"]),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


# ──────────────────────────────────────────────
# Callback: Logs & Console
# ──────────────────────────────────────────────
@router.callback_query(F.data.startswith("bot_logs:"))
async def cb_bot_logs(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS} Доступ запрещён", show_alert=True)
        return

    logs = runner.get_logs(cb.from_user.id, bot_id)
    try:
        await cb.message.edit_text(
            utils.format_logs(bot_data["name"], logs),
            reply_markup=utils.bot_logs_keyboard(bot_id),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    await cb.answer()


@router.callback_query(F.data.startswith("console_refresh:"))
async def cb_console_refresh(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return
    logs = runner.get_logs(cb.from_user.id, bot_id)
    try:
        await cb.message.edit_text(
            utils.format_logs(bot_data["name"], logs),
            reply_markup=utils.console_keyboard(bot_id),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    await cb.answer(f"{SYM_REFRESH} Обновлено")


# ──────────────────────────────────────────────
# Callback: ENV Editor
# ──────────────────────────────────────────────
@router.callback_query(F.data.startswith("bot_env:"))
async def cb_bot_env(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    env_vars = db.get_env(bot_id)
    await cb.message.edit_text(
        utils.format_env(bot_data["name"], env_vars),
        reply_markup=utils.env_keyboard(bot_id, env_vars),
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


@router.callback_query(F.data.startswith("env_add:"))
async def cb_env_add(cb: CallbackQuery, state: FSMContext):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    await state.set_state(EditEnv.waiting_key)
    await state.update_data(bot_id=bot_id, mode="add")
    await cb.message.edit_text(
        f"{SYM_EDIT} <b>Добавить переменную</b>\n\nВведите <b>имя</b> переменной (KEY):",
        parse_mode=ParseMode.HTML,
        reply_markup=utils.cancel_keyboard(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("env_edit:"))
async def cb_env_edit_var(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    bot_id, key = int(parts[1]), parts[2]
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    await state.set_state(EditEnv.waiting_value)
    await state.update_data(bot_id=bot_id, env_key=key)
    await cb.message.edit_text(
        f"{SYM_EDIT} <b>Редактировать</b> <code>{key}</code>\n\nВведите новое значение:",
        parse_mode=ParseMode.HTML,
        reply_markup=utils.cancel_keyboard(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("env_del:"))
async def cb_env_del(cb: CallbackQuery):
    parts = cb.data.split(":")
    bot_id, key = int(parts[1]), parts[2]
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    db.delete_env_var(bot_id, key)
    runner.write_env_file(cb.from_user.id, bot_id)
    env_vars = db.get_env(bot_id)
    try:
        await cb.message.edit_text(
            utils.format_env(bot_data["name"], env_vars),
            reply_markup=utils.env_keyboard(bot_id, env_vars),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    await cb.answer(f"{SYM_DELETE} Удалено: {key}")


@router.callback_query(F.data.startswith("env_edit_all:"))
async def cb_env_edit_all(cb: CallbackQuery, state: FSMContext):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    env_vars = db.get_env(bot_id)
    env_text = "\n".join(f"{k}={v}" for k, v in env_vars.items())

    await state.set_state(EditEnv.waiting_all_env)
    await state.update_data(bot_id=bot_id)
    await cb.message.edit_text(
        f"{SYM_LIST} <b>Редактировать все ENV</b>\n\n"
        f"Текущие переменные:\n<pre>{env_text or '(пусто)'}</pre>\n\n"
        f"Отправьте новый список в формате KEY=VALUE (каждая с новой строки):",
        parse_mode=ParseMode.HTML,
        reply_markup=utils.cancel_keyboard(),
    )
    await cb.answer()


# FSM: ENV key input
@router.message(EditEnv.waiting_key)
async def fsm_env_key(msg: Message, state: FSMContext):
    key = msg.text.strip().upper()
    if not key.replace("_", "").isalnum():
        await msg.answer(f"{SYM_CROSS} Неверный формат имени. Используйте A-Z, 0-9, _")
        return
    await state.update_data(env_key=key)
    await state.set_state(EditEnv.waiting_value)
    await msg.answer(
        f"{SYM_EDIT} Введите значение для <code>{key}</code>:",
        parse_mode=ParseMode.HTML,
        reply_markup=utils.cancel_keyboard(),
    )


# FSM: ENV value input
@router.message(EditEnv.waiting_value)
async def fsm_env_value(msg: Message, state: FSMContext):
    data = await state.get_data()
    bot_id = data["bot_id"]
    key = data["env_key"]
    value = msg.text.strip()

    db.set_env_var(bot_id, key, value)
    runner.write_env_file(msg.from_user.id, bot_id)
    await state.clear()

    bot_data = db.get_bot(bot_id)
    env_vars = db.get_env(bot_id)
    await msg.answer(
        f"{SYM_CHECK} Переменная <code>{key}</code> сохранена!\n\n" +
        utils.format_env(bot_data["name"], env_vars),
        reply_markup=utils.env_keyboard(bot_id, env_vars),
        parse_mode=ParseMode.HTML,
    )


# FSM: Edit all ENV
@router.message(EditEnv.waiting_all_env)
async def fsm_env_all(msg: Message, state: FSMContext):
    data = await state.get_data()
    bot_id = data["bot_id"]

    env_dict = utils.parse_env_text(msg.text)
    db.set_env_bulk(bot_id, env_dict)
    runner.write_env_file(msg.from_user.id, bot_id)
    await state.clear()

    bot_data = db.get_bot(bot_id)
    env_vars = db.get_env(bot_id)
    await msg.answer(
        f"{SYM_CHECK} ENV переменные обновлены ({len(env_dict)} шт.)\n\n" +
        utils.format_env(bot_data["name"], env_vars),
        reply_markup=utils.env_keyboard(bot_id, env_vars),
        parse_mode=ParseMode.HTML,
    )


# ──────────────────────────────────────────────
# Callback: File Editor
# ──────────────────────────────────────────────
@router.callback_query(F.data.startswith("bot_files:"))
async def cb_bot_files(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    files = runner.list_editable_files(cb.from_user.id, bot_id)
    if not files:
        await cb.answer("Нет редактируемых файлов", show_alert=True)
        return

    await cb.message.edit_text(
        f"{SYM_FOLDER} <b>Файлы бота</b>: {bot_data['name']}\n\nВыберите файл для просмотра:",
        reply_markup=utils.files_keyboard(bot_id, files),
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


@router.callback_query(F.data.startswith("file_view:"))
async def cb_file_view(cb: CallbackQuery):
    parts = cb.data.split(":", 2)
    bot_id, safe_name = int(parts[1]), parts[2]
    filename = safe_name.replace("_", "/", 1) if safe_name.startswith(".") else safe_name

    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    ok, content = runner.read_file(cb.from_user.id, bot_id, filename)
    if not ok:
        await cb.answer(f"{SYM_CROSS} {content}", show_alert=True)
        return

    MAX_PREVIEW = 3000
    preview = content[:MAX_PREVIEW]
    if len(content) > MAX_PREVIEW:
        preview += "\n... (обрезано)"

    await cb.message.edit_text(
        f"{SYM_FILE} <b>{filename}</b>\n\n<pre>{utils._escape_html(preview)}</pre>",
        reply_markup=utils.file_view_keyboard(bot_id, filename),
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


@router.callback_query(F.data.startswith("file_edit:"))
async def cb_file_edit(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":", 2)
    bot_id, safe_name = int(parts[1]), parts[2]
    filename = safe_name

    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    ok, content = runner.read_file(cb.from_user.id, bot_id, filename)
    if not ok:
        await cb.answer(f"{SYM_CROSS} {content}", show_alert=True)
        return

    await state.set_state(EditFile.waiting_content)
    await state.update_data(bot_id=bot_id, filename=filename)
    await cb.message.edit_text(
        f"{SYM_EDIT} <b>Редактировать</b>: <code>{filename}</code>\n\n"
        f"Отправьте новое содержимое файла.\n\n"
        f"<i>Текущее содержимое:</i>\n<pre>{utils._escape_html(content[:1000])}</pre>",
        parse_mode=ParseMode.HTML,
        reply_markup=utils.cancel_keyboard(),
    )
    await cb.answer()


@router.message(EditFile.waiting_content)
async def fsm_file_content(msg: Message, state: FSMContext):
    data = await state.get_data()
    bot_id = data["bot_id"]
    filename = data["filename"]

    ok, result = runner.write_file(msg.from_user.id, bot_id, filename, msg.text)
    await state.clear()

    if ok:
        await msg.answer(
            f"{SYM_CHECK} Файл <code>{filename}</code> сохранён!",
            parse_mode=ParseMode.HTML,
            reply_markup=utils.main_keyboard(),
        )
    else:
        await msg.answer(f"{SYM_CROSS} Ошибка сохранения: {result}")


# ──────────────────────────────────────────────
# Callback: Delete bot
# ──────────────────────────────────────────────
@router.callback_query(F.data.startswith("bot_delete_confirm:"))
async def cb_delete_confirm(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    await cb.message.edit_text(
        f"{SYM_DELETE} <b>Удалить бота</b>: {bot_data['name']}\n\n"
        f"{SYM_WARNING} Это действие необратимо!\n"
        "Все файлы и данные будут удалены.",
        reply_markup=utils.confirm_delete_keyboard(bot_id),
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()


@router.callback_query(F.data.startswith("bot_delete:"))
async def cb_delete_bot(cb: CallbackQuery):
    bot_id = int(cb.data.split(":")[1])
    bot_data = db.get_bot(bot_id)
    if not bot_data or bot_data["user_id"] != cb.from_user.id:
        await cb.answer(f"{SYM_CROSS}")
        return

    # Stop if running
    await runner.stop_bot(bot_id)

    # Remove files
    import shutil
    bot_path = runner.bot_dir(cb.from_user.id, bot_id)
    shutil.rmtree(bot_path, ignore_errors=True)

    # Remove from DB
    db.delete_bot(bot_id)

    await cb.message.edit_text(
        f"{SYM_DELETE} Бот <b>{bot_data['name']}</b> удалён.",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer(f"{SYM_CHECK} Удалено")


# ──────────────────────────────────────────────
# ➕ Create Bot FSM
# ──────────────────────────────────────────────
@router.message(F.text == f"{SYM_PLUS} Создать бота")
async def cmd_create_bot(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    user = db.get_user(user_id)
    max_bots = user.get("max_bots", MAX_BOTS_PER_USER) if user else MAX_BOTS_PER_USER

    if db.count_user_bots(user_id) >= max_bots:
        await msg.answer(
            f"{SYM_CROSS} Достигнут лимит ботов ({max_bots}).\n"
            "Удалите неиспользуемых ботов.",
            reply_markup=utils.main_keyboard(),
        )
        return

    if not db.check_rate_limit(user_id, "create_bot"):
        await msg.answer(
            f"{SYM_WAIT} Слишком много попыток создания. Подождите немного.",
            reply_markup=utils.main_keyboard(),
        )
        return

    await state.set_state(CreateBot.waiting_source)
    await msg.answer(
        utils.format_create_step(1, {}),
        parse_mode=ParseMode.HTML,
        reply_markup=utils.cancel_keyboard(),
    )


# Step 1: GitHub URL or ZIP
@router.message(CreateBot.waiting_source)
async def fsm_create_source(msg: Message, state: FSMContext):
    # Handle document (ZIP)
    if msg.document:
        doc: Document = msg.document
        if not doc.file_name.endswith(".zip"):
            await msg.answer(f"{SYM_CROSS} Поддерживаются только .zip файлы.")
            return
        if doc.file_size and doc.file_size > 50 * 1024 * 1024:
            await msg.answer(f"{SYM_CROSS} Файл слишком большой (макс 50MB).")
            return
        await state.update_data(source_type="zip", file_id=doc.file_id)

    # Handle text URL
    elif msg.text:
        url = msg.text.strip()
        if not utils.is_valid_github_url(url):
            await msg.answer(
                f"{SYM_CROSS} Неверный URL. Отправьте ссылку GitHub:\n"
                "<code>https://github.com/user/repo</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        await state.update_data(source_type="github", url=url)
    else:
        await msg.answer(f"{SYM_CROSS} Отправьте GitHub ссылку или ZIP архив.")
        return

    await state.set_state(CreateBot.waiting_name)
    await msg.answer(
        utils.format_create_step(3, {}),
        parse_mode=ParseMode.HTML,
        reply_markup=utils.cancel_keyboard(),
    )


# Step 2: Bot name
@router.message(CreateBot.waiting_name)
async def fsm_create_name(msg: Message, state: FSMContext):
    ok, name = utils.validate_bot_name(msg.text or "")
    if not ok:
        await msg.answer(f"{SYM_CROSS} {name}")
        return

    await state.update_data(bot_name=name)
    await state.set_state(CreateBot.waiting_mainfile)
    await msg.answer(
        utils.format_create_step(2, {}),
        parse_mode=ParseMode.HTML,
        reply_markup=utils.cancel_keyboard(),
    )


# Step 3: Main file
@router.message(CreateBot.waiting_mainfile)
async def fsm_create_mainfile(msg: Message, state: FSMContext):
    mainfile = (msg.text or "").strip()
    if not mainfile.endswith(".py"):
        await msg.answer(f"{SYM_CROSS} Укажите .py файл, например: <code>main.py</code>", parse_mode=ParseMode.HTML)
        return

    await state.update_data(main_file=mainfile)
    await state.set_state(CreateBot.waiting_env)

    builder = InlineKeyboardBuilder()
    builder.button(text="⏭ Пропустить", callback_data="create_skip_env")
    builder.button(text=f"{SYM_CROSS} Отмена", callback_data="cancel")

    await msg.answer(
        utils.format_create_step(4, {}),
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup(),
    )


# Step 4: ENV vars
@router.message(CreateBot.waiting_env)
async def fsm_create_env(msg: Message, state: FSMContext):
    env_dict = utils.parse_env_text(msg.text or "")
    await _finish_create_bot(msg, state, env_dict)


@router.callback_query(F.data == "create_skip_env")
async def cb_skip_env(cb: CallbackQuery, state: FSMContext):
    current = await state.get_state()
    if current != CreateBot.waiting_env:
        await cb.answer()
        return
    await cb.answer()
    await _finish_create_bot(cb.message, state, {}, user_id=cb.from_user.id)


async def _finish_create_bot(msg: Message, state: FSMContext, env_dict: dict, user_id: int = None):
    data = await state.get_data()
    await state.clear()

    uid = user_id or msg.chat.id
    bot_name = data.get("bot_name", "MyBot")
    main_file = data.get("main_file", "main.py")
    source_type = data.get("source_type")

    status_msg = await msg.answer(
        f"{SYM_SETTINGS} <b>Создаём бота</b>: {bot_name}\n\n{SYM_WAIT} Загрузка файлов...",
        parse_mode=ParseMode.HTML,
        reply_markup=utils.main_keyboard(),
    )

    # Create DB record
    bot_id = db.create_bot(uid, bot_name, main_file)

    # Set env
    if env_dict:
        db.set_env_bulk(bot_id, env_dict)

    # Create directory
    project = runner.bot_dir(uid, bot_id)
    project.mkdir(parents=True, exist_ok=True)

    # Download/extract source
    asyncio.create_task(
        _setup_bot_task(uid, bot_id, bot_name, source_type, data, status_msg)
    )


async def _setup_bot_task(user_id: int, bot_id: int, bot_name: str,
                           source_type: str, data: dict, status_msg: Message):
    async def update_status(text: str):
        nonlocal status_msg
        try:
            await status_msg.edit_text(text, parse_mode=ParseMode.HTML)
        except Exception:
            # If edit fails (e.g. message not found), send a new status message.
            try:
                status_msg = await status_msg.answer(text, parse_mode=ParseMode.HTML)
            except Exception:
                pass

    project = runner.bot_dir(user_id, bot_id)

    try:
        if source_type == "github":
            await update_status(
                f"{SYM_SETTINGS} <b>{bot_name}</b>\n\n{SYM_DOWNLOAD} Скачиваем с GitHub...\n"
                f"<code>{data['url']}</code>"
            )
            ok, err = await runner.download_github(data["url"], project)
        elif source_type == "zip":
            await update_status(f"{SYM_SETTINGS} <b>{bot_name}</b>\n\n{SYM_PACKAGE} Распаковываем архив...")
            # Download from Telegram
            from aiogram import Bot as ABot
            abot = ABot(token=BOT_TOKEN)
            file = await abot.get_file(data["file_id"])
            tmp_zip = Path(f"tmp/bot_{bot_id}.zip")
            await abot.download_file(file.file_path, destination=str(tmp_zip))
            await abot.session.close()
            ok, err = await runner.extract_zip(tmp_zip, project)
        else:
            ok, err = False, "Unknown source type"

        if not ok:
            db.delete_bot(bot_id)
            import shutil
            shutil.rmtree(project, ignore_errors=True)
            await update_status(f"{SYM_CROSS} <b>Ошибка загрузки</b>\n\n{err}")
            return

        # Validate files
        valid, reason = runner.validate_project_dir(project)
        if not valid:
            db.delete_bot(bot_id)
            import shutil
            shutil.rmtree(project, ignore_errors=True)
            await update_status(f"{SYM_CROSS} <b>Проверка безопасности не пройдена</b>\n\n{reason}")
            return

        await update_status(f"{SYM_SETTINGS} <b>{bot_name}</b>\n\n{SYM_PACKAGE} Устанавливаем зависимости...")

        ok, err = await runner.install_dependencies(user_id, bot_id)
        if not ok:
            await update_status(
                f"{SYM_WARNING} <b>{bot_name}</b>\n\n"
                f"Установка зависимостей завершилась с предупреждением.\n"
                f"Бот создан и может работать. Подробности смотрите в логах установки."
            )
        else:
            await update_status(
                f"{SYM_CHECK} <b>{bot_name}</b> создан!\n\n"
                f"📄 Файл запуска: <code>{data.get('main_file', 'main.py')}</code>\n"
                f"🚀 Нажмите <b>▶ Запустить</b> в меню бота.",
            )

    except Exception as e:
        logger.exception("Error setting up bot %d", bot_id)
        await update_status(f"{SYM_CROSS} Критическая ошибка: {e}")


# ──────────────────────────────────────────────
# Cancel FSM
# ──────────────────────────────────────────────
@router.callback_query(F.data == "cancel")
async def cb_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(f"{SYM_CROSS} Отменено.")
    await cb.answer()


@router.message(Command("cancel"))
async def cmd_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(f"{SYM_CROSS} Отменено.", reply_markup=utils.main_keyboard())


# ──────────────────────────────────────────────
# Admin commands
# ──────────────────────────────────────────────
@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    import shutil
    stats = runner.get_system_stats()
    bots_count = sum(1 for _ in Path("bots").rglob("*/"))
    disk = shutil.disk_usage(".")
    await msg.answer(
        f"{SYM_ADMIN} <b>Панель администратора</b>\n\n"
        f"{SYM_DISK} Диск: {disk.used // 1024 // 1024} / {disk.total // 1024 // 1024} MB\n"
        f"{SYM_BOT} Процессов: {len(runner._processes)}\n"
        f"{SYM_DISK} RAM: {stats.get('ram_used_mb', 0)} / {stats.get('ram_total_mb', 0)} MB\n"
        f"{SYM_CPU} CPU: {stats.get('cpu_percent', 0)}%\n\n"
        f"/admin_ban [user_id] — Заблокировать\n"
        f"/admin_stats — Полная статистика",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("admin_ban"))
async def cmd_admin_ban(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    parts = msg.text.split()
    if len(parts) < 2:
        await msg.answer("Использование: /admin_ban [user_id]")
        return
    try:
        target_id = int(parts[1])
        with db.get_conn() as conn:
            conn.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (target_id,))
        db._cache_del(f"user:{target_id}")
        await msg.answer(f"{SYM_CHECK} Пользователь {target_id} заблокирован.")
    except ValueError:
        await msg.answer(f"{SYM_CROSS} Неверный ID")


# ──────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────
async def main():
    Path("data").mkdir(exist_ok=True)

    # Init DB
    db.init_db()

    # Restore bot states
    await runner.restore_running_bots()

    # Create Bot + Dispatcher
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    # Middlewares
    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())
    dp.message.middleware(RateLimitMiddleware())
    dp.callback_query.middleware(RateLimitMiddleware())

    dp.include_router(router)

    # Background tasks
    asyncio.create_task(runner.periodic_cleanup())

    logger.info("🚀 Bot Hosting started")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())