# 🚀 Telegram Bot Hosting

Полноценный хостинг Telegram-ботов прямо через Telegram.  
Управляйте своими ботами через красивый интерфейс с inline-кнопками, консолью и редактором файлов.

---

## 📁 Структура проекта

```
telegram-bot-hosting/
├── bot.py              # Telegram-бот (интерфейс, FSM, хендлеры)
├── runner.py           # Запуск процессов, безопасность, логи
├── db.py               # SQLite + кэширование
├── utils.py            # Клавиатуры, форматирование
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

---

## ⚡ Быстрый старт

### 1. Клонировать и настроить

```bash
git clone <repo>
cd telegram-bot-hosting
cp .env.example .env
nano .env   # вставьте BOT_TOKEN и ADMIN_IDS
```

### 2. Запуск через Docker (рекомендуется)

```bash
docker build -t bothost .
docker run -d \
  --name bothost \
  --env-file .env \
  -v $(pwd)/bots:/app/bots \
  -v $(pwd)/data:/app/data \
  --restart unless-stopped \
  bothost
```

### Или через docker-compose

```bash
docker-compose up -d
```

### Без Docker (прямо на VPS)

```bash
pip install -r requirements.txt
python bot.py
```

---

## 🎨 Интерфейс

### Главное меню
```
🤖 Мои боты     |  ➕ Создать бота
📊 Статус        |  🖥 Консоль
         ⚙ Настройки
```

### Управление ботом
```
▶ Запустить   ⏹ Остановить
🔄 Перезапустить
📜 Логи       ✏ ENV
📁 Файлы      🗑 Удалить
```

---

## 🏗 Архитектура

### `db.py` — База данных
- SQLite с WAL-режимом (высокая конкурентность)
- In-memory кэш (TTL 30 сек) — минимизирует I/O
- Rate limiting в БД (action / bot_start / create_bot)
- Таблицы: `users`, `bots`, `bot_env`, `rate_limits`

### `runner.py` — Запуск ботов
- `subprocess.Popen` с `preexec_fn` для resource limits
- `os.killpg` — убивает всю группу процессов (включая дочерние)
- Авто-перезапуск при краше (до 5 раз)
- Semaphore-очереди: MAX_INSTALL=2, MAX_START=3
- Логи в `bots/{user_id}/{bot_id}/logs.txt`

### `bot.py` — Telegram интерфейс
- aiogram 3.x, полностью async
- FSM для создания ботов (4 шага)
- Middleware: регистрация пользователей + rate limit
- `asyncio.create_task()` для тяжёлых операций

### `utils.py` — Хелперы
- Все клавиатуры (InlineKeyboard + ReplyKeyboard)
- Форматирование сообщений
- Валидация данных

---

## 🔐 Безопасность

| Механизм | Реализация |
|---|---|
| Resource limits | `resource.setrlimit` (CPU, RAM, файлы, процессы) |
| Изоляция процессов | `start_new_session=True`, отдельный PGID |
| Запрет shell-файлов | `.sh`, `.bash`, `.service` и др. |
| Размер проекта | Макс 50 MB |
| Path traversal | Проверка `file_path.startswith(project_dir)` |
| Rate limiting | 10 действий/мин, 3 запуска/мин |
| Бан пользователей | `/admin_ban [user_id]` |

---

## 📊 Лимиты по умолчанию

| Параметр | Значение |
|---|---|
| Ботов на пользователя | 5 |
| Размер проекта | 50 MB |
| RAM на бот-процесс | 256 MB |
| CPU-время | 3600 сек/час |
| Макс. процессов (fork) | 50 |
| Параллельных установок | 2 |
| Параллельных запусков | 3 |
| Авто-рестартов | 5 |

---

## 🔧 Переменные окружения

```env
BOT_TOKEN=          # Токен хостинг-бота
ADMIN_IDS=          # ID администраторов (через запятую)
MAX_BOTS_PER_USER=5 # Лимит ботов на пользователя
```

---

## 📋 Структура хранения

```
bots/
└── {user_id}/
    └── {bot_id}/
        ├── main.py        # Файлы бота
        ├── requirements.txt
        ├── .env           # ENV переменные
        └── logs.txt       # Логи

data/
├── bothost.db     # SQLite база
└── bot_host.log   # Системный лог
```

---

## 👑 Команды администратора

```
/admin           — Панель администратора
/admin_ban [id]  — Заблокировать пользователя
```
