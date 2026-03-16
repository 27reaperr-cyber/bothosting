"""
runner.py — Bot process management.
Handles: start/stop/restart, sandbox security, log capture, monitoring.
NO Docker, NO docker.sock — pure subprocess + resource limits.
"""

import asyncio
import logging
import os
import re
import resource
import shutil
import signal
import subprocess
import sys
import time
import zipfile
from collections import deque
from pathlib import Path
from typing import Optional

import aiohttp
import aiofiles

import db

logger = logging.getLogger("bothost.runner")

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
BOTS_DIR = Path("bots")
MAX_BOT_SIZE_BYTES = 50 * 1024 * 1024   # 50 MB
MAX_LOG_LINES = 500
LOG_TAIL_LINES = 50
MAX_PROCESSES_PER_BOT = 1

# Resource limits per bot process
RLIMIT_CPU_SECONDS = 3600          # 1 hour CPU time
RLIMIT_MAX_PROCESSES = 50          # no fork bomb
RLIMIT_MAX_OPEN_FILES = 64
RLIMIT_MAX_MEMORY = 256 * 1024 * 1024  # 256 MB RAM

FORBIDDEN_EXTENSIONS = {".sh", ".bash", ".service", ".systemd", ".socket", ".timer"}
FORBIDDEN_FILENAMES = {"Makefile", "Dockerfile", ".bashrc", ".bash_profile"}

# Semaphores for task queues
MAX_INSTALL_TASKS = 2
MAX_START_TASKS = 3
_install_semaphore: asyncio.Semaphore = None
_start_semaphore: asyncio.Semaphore = None

# Active process table: bot_id -> subprocess.Popen
_processes: dict[int, subprocess.Popen] = {}

# In-memory log ring buffers: bot_id -> deque[str]
_log_buffers: dict[int, deque] = {}


def get_semaphores():
    global _install_semaphore, _start_semaphore
    if _install_semaphore is None:
        _install_semaphore = asyncio.Semaphore(MAX_INSTALL_TASKS)
    if _start_semaphore is None:
        _start_semaphore = asyncio.Semaphore(MAX_START_TASKS)
    return _install_semaphore, _start_semaphore


# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
def bot_dir(user_id: int, bot_id: int) -> Path:
    return BOTS_DIR / str(user_id) / str(bot_id)


def bot_log_path(user_id: int, bot_id: int) -> Path:
    return bot_dir(user_id, bot_id) / "logs.txt"


def bot_env_path(user_id: int, bot_id: int) -> Path:
    return bot_dir(user_id, bot_id) / ".env"


# ──────────────────────────────────────────────
# Security checks
# ──────────────────────────────────────────────
def validate_project_dir(project_path: Path) -> tuple[bool, str]:
    """
    Scan extracted project for forbidden files.
    Returns (ok, reason).
    """
    total_size = 0
    for f in project_path.rglob("*"):
        if f.is_file():
            if f.suffix.lower() in FORBIDDEN_EXTENSIONS:
                return False, f"Forbidden file type: {f.name}"
            if f.name in FORBIDDEN_FILENAMES:
                return False, f"Forbidden file: {f.name}"
            total_size += f.stat().st_size
            if total_size > MAX_BOT_SIZE_BYTES:
                return False, "Project exceeds 50MB limit"
    return True, "ok"


def _set_resource_limits():
    """Called in child process via preexec_fn."""
    try:
        # CPU time
        resource.setrlimit(resource.RLIMIT_CPU, (RLIMIT_CPU_SECONDS, RLIMIT_CPU_SECONDS))
        # Max processes (anti fork-bomb)
        resource.setrlimit(resource.RLIMIT_NPROC, (RLIMIT_MAX_PROCESSES, RLIMIT_MAX_PROCESSES))
        # Open files
        resource.setrlimit(resource.RLIMIT_NOFILE, (RLIMIT_MAX_OPEN_FILES, RLIMIT_MAX_OPEN_FILES))
        # Address space / RAM
        resource.setrlimit(resource.RLIMIT_AS, (RLIMIT_MAX_MEMORY, RLIMIT_MAX_MEMORY))
        # Core dumps off
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    except Exception as e:
        # Don't prevent start, just log
        pass


# ──────────────────────────────────────────────
# GitHub / ZIP download
# ──────────────────────────────────────────────
async def download_github(url: str, dest: Path) -> tuple[bool, str]:
    """
    Clone or download GitHub repo as zip.
    Supports: https://github.com/user/repo or https://github.com/user/repo/tree/branch
    """
    try:
        # Normalize URL to zip download
        url = url.strip().rstrip("/")
        match = re.match(r"https://github\.com/([^/]+)/([^/]+)(?:/tree/([^/]+))?", url)
        if not match:
            return False, "Invalid GitHub URL"
        user, repo, branch = match.group(1), match.group(2), match.group(3) or "main"
        repo = repo.replace(".git", "")

        zip_url = f"https://github.com/{user}/{repo}/archive/refs/heads/{branch}.zip"
        tmp_zip = dest.parent / f"{dest.name}_tmp.zip"

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.get(zip_url) as resp:
                if resp.status == 404:
                    # Try 'master' branch
                    zip_url = f"https://github.com/{user}/{repo}/archive/refs/heads/master.zip"
                    async with session.get(zip_url) as resp2:
                        if resp2.status != 200:
                            return False, f"Repository not found (tried main/master)"
                        content = await resp2.read()
                elif resp.status != 200:
                    return False, f"GitHub returned {resp.status}"
                else:
                    content = await resp.read()

        if len(content) > MAX_BOT_SIZE_BYTES:
            return False, "Archive exceeds 50MB limit"

        async with aiofiles.open(tmp_zip, "wb") as f:
            await f.write(content)

        return await extract_zip(tmp_zip, dest)
    except asyncio.TimeoutError:
        return False, "Download timed out (60s)"
    except Exception as e:
        return False, str(e)


async def extract_zip(zip_path: Path, dest: Path) -> tuple[bool, str]:
    """Extract zip archive to dest, flatten single top-level dir."""
    try:
        if not zipfile.is_zipfile(zip_path):
            return False, "Not a valid ZIP file"

        tmp_extract = dest.parent / f"{dest.name}_extract"
        shutil.rmtree(tmp_extract, ignore_errors=True)
        tmp_extract.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zf:
            total = sum(info.file_size for info in zf.infolist())
            if total > MAX_BOT_SIZE_BYTES:
                return False, "Extracted size exceeds 50MB limit"
            zf.extractall(tmp_extract)

        # Flatten: if single top-level dir, move it to dest; otherwise move tmp_extract.
        top_dirs = list(tmp_extract.iterdir())
        src_root = top_dirs[0] if len(top_dirs) == 1 and top_dirs[0].is_dir() else tmp_extract

        # Ensure dest is clean so we don't nest into an existing folder.
        if dest.exists():
            shutil.rmtree(dest, ignore_errors=True)

        shutil.move(str(src_root), str(dest))

        # Cleanup leftover temp folder if needed
        if tmp_extract.exists():
            shutil.rmtree(tmp_extract, ignore_errors=True)

        zip_path.unlink(missing_ok=True)
        return True, "ok"
    except Exception as e:
        return False, str(e)


# ──────────────────────────────────────────────
# Dependency installation
# ──────────────────────────────────────────────
async def install_dependencies(user_id: int, bot_id: int) -> tuple[bool, str]:
    sem, _ = get_semaphores()
    async with sem:
        project = bot_dir(user_id, bot_id)
        req_file = project / "requirements.txt"
        if not req_file.exists():
            return True, "No requirements.txt found, skipping"

        log_path = bot_log_path(user_id, bot_id)
        _append_log(bot_id, "📦 Installing dependencies...")

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "pip", "install",
                "-r", str(req_file),
                "--no-cache-dir", "--quiet",
                cwd=str(project),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)

            output = stdout.decode(errors="replace") + stderr.decode(errors="replace")
            for line in output.splitlines():
                _append_log(bot_id, line)

            if proc.returncode != 0:
                return False, f"pip install failed (exit {proc.returncode})"

            _append_log(bot_id, "✅ Dependencies installed successfully")
            return True, "ok"
        except asyncio.TimeoutError:
            return False, "Installation timed out (180s)"
        except Exception as e:
            return False, str(e)


# ──────────────────────────────────────────────
# ENV file write
# ──────────────────────────────────────────────
def write_env_file(user_id: int, bot_id: int):
    env_vars = db.get_env(bot_id)
    env_path = bot_env_path(user_id, bot_id)
    lines = [f'{k}={v}\n' for k, v in env_vars.items()]
    env_path.write_text("".join(lines), encoding="utf-8")


# ──────────────────────────────────────────────
# Start / Stop / Restart
# ──────────────────────────────────────────────
async def start_bot(user_id: int, bot_id: int) -> tuple[bool, str]:
    _, sem = get_semaphores()
    async with sem:
        return await _do_start(user_id, bot_id)


async def _do_start(user_id: int, bot_id: int) -> tuple[bool, str]:
    bot = db.get_bot(bot_id)
    if not bot:
        return False, "Bot not found"

    if bot_id in _processes:
        proc = _processes[bot_id]
        if proc.poll() is None:
            return False, "Bot is already running"

    project = bot_dir(user_id, bot_id)
    main_file = project / bot["main_file"]

    if not main_file.exists():
        return False, f"Main file not found: {bot['main_file']}"

    ok, reason = validate_project_dir(project)
    if not ok:
        return False, reason

    # Write fresh .env
    write_env_file(user_id, bot_id)

    env = os.environ.copy()
    env_vars = db.get_env(bot_id)
    env.update(env_vars)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONUNBUFFERED"] = "1"

    log_path = bot_log_path(user_id, bot_id)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        log_file = open(log_path, "a", encoding="utf-8")
        ts = time.strftime("%H:%M:%S")
        log_file.write(f"[{ts}] 🚀 Bot started\n")
        log_file.flush()

        proc = subprocess.Popen(
            [sys.executable, "-u", bot["main_file"]],
            cwd=str(project),
            env=env,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,   # isolate process group
            preexec_fn=_set_resource_limits,
        )

        _processes[bot_id] = proc
        _log_files[bot_id] = log_file

        db.update_bot_status(bot_id, "running", proc.pid)
        _append_log(bot_id, f"[{ts}] 🚀 Started with PID {proc.pid}")
        logger.info("Bot %d started (PID %d)", bot_id, proc.pid)

        # Spawn async watcher
        asyncio.create_task(_watch_bot(user_id, bot_id))
        return True, f"Bot started (PID {proc.pid})"
    except Exception as e:
        db.update_bot_status(bot_id, "error")
        return False, str(e)


# Track open log file handles
_log_files: dict[int, object] = {}


async def stop_bot(bot_id: int) -> tuple[bool, str]:
    proc = _processes.get(bot_id)
    if proc is None or proc.poll() is not None:
        db.update_bot_status(bot_id, "stopped")
        return True, "Bot was not running"

    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        pass

    # Wait up to 5s for graceful shutdown
    try:
        await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, proc.wait), timeout=5)
    except asyncio.TimeoutError:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass

    _processes.pop(bot_id, None)
    lf = _log_files.pop(bot_id, None)
    if lf:
        try:
            lf.close()
        except Exception:
            pass

    db.update_bot_status(bot_id, "stopped")
    ts = time.strftime("%H:%M:%S")
    _append_log(bot_id, f"[{ts}] ⏹ Bot stopped")
    logger.info("Bot %d stopped", bot_id)
    return True, "Bot stopped"


async def restart_bot(user_id: int, bot_id: int) -> tuple[bool, str]:
    await stop_bot(bot_id)
    await asyncio.sleep(1)
    return await start_bot(user_id, bot_id)


# ──────────────────────────────────────────────
# Process watcher (auto-restart)
# ──────────────────────────────────────────────
async def _watch_bot(user_id: int, bot_id: int):
    """Monitors a bot process, auto-restarts on crash."""
    while True:
        await asyncio.sleep(5)
        proc = _processes.get(bot_id)
        if proc is None:
            break
        if proc.poll() is not None:
            # Process died
            bot = db.get_bot(bot_id)
            if not bot or bot["status"] == "stopped":
                break  # Intentional stop

            db.increment_restarts(bot_id)
            bot = db.get_bot(bot_id)
            restarts = bot["restarts"]
            ts = time.strftime("%H:%M:%S")
            _append_log(bot_id, f"[{ts}] 💀 Bot crashed (exit {proc.returncode}), restart #{restarts}")
            logger.warning("Bot %d crashed (exit %d), restart #%d", bot_id, proc.returncode, restarts)

            if restarts >= bot.get("max_restarts", 5):
                db.update_bot_status(bot_id, "error")
                _append_log(bot_id, f"[{ts}] ❌ Max restarts reached, bot stopped")
                _processes.pop(bot_id, None)
                break

            await asyncio.sleep(3)  # brief delay before restart
            ok, msg = await _do_start(user_id, bot_id)
            if not ok:
                _append_log(bot_id, f"[{ts}] ❌ Restart failed: {msg}")
                break


# ──────────────────────────────────────────────
# Log management
# ──────────────────────────────────────────────
def _append_log(bot_id: int, line: str):
    if bot_id not in _log_buffers:
        _log_buffers[bot_id] = deque(maxlen=MAX_LOG_LINES)
    _log_buffers[bot_id].append(line)


def get_logs(user_id: int, bot_id: int, lines: int = LOG_TAIL_LINES) -> str:
    log_path = bot_log_path(user_id, bot_id)
    if not log_path.exists():
        return "📭 No logs yet"

    try:
        all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        tail = all_lines[-lines:]
        return "\n".join(tail) if tail else "📭 No logs yet"
    except Exception as e:
        return f"⚠️ Could not read logs: {e}"


def clear_old_logs(user_id: int, bot_id: int, keep_lines: int = MAX_LOG_LINES):
    log_path = bot_log_path(user_id, bot_id)
    if not log_path.exists():
        return
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if len(lines) > keep_lines:
            log_path.write_text("\n".join(lines[-keep_lines:]) + "\n", encoding="utf-8")
    except Exception:
        pass


# ──────────────────────────────────────────────
# File editor
# ──────────────────────────────────────────────
EDITABLE_EXTENSIONS = {".py", ".txt", ".json", ".env", ".cfg", ".ini", ".yaml", ".yml", ".toml", ".md"}
MAX_EDIT_FILE_SIZE = 64 * 1024  # 64 KB


def list_editable_files(user_id: int, bot_id: int) -> list[str]:
    project = bot_dir(user_id, bot_id)
    if not project.exists():
        return []
    files = []
    for f in sorted(project.rglob("*")):
        if f.is_file() and f.suffix.lower() in EDITABLE_EXTENSIONS:
            rel = str(f.relative_to(project))
            if not rel.startswith("."):
                files.append(rel)
            elif f.name == ".env":
                files.insert(0, ".env")
    return files[:20]  # limit


def read_file(user_id: int, bot_id: int, filename: str) -> tuple[bool, str]:
    project = bot_dir(user_id, bot_id)
    file_path = (project / filename).resolve()

    # Path traversal protection
    if not str(file_path).startswith(str(project.resolve())):
        return False, "Access denied"

    if not file_path.exists():
        return False, "File not found"

    if file_path.stat().st_size > MAX_EDIT_FILE_SIZE:
        return False, "File too large to edit (>64KB)"

    try:
        return True, file_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return False, str(e)


def write_file(user_id: int, bot_id: int, filename: str, content: str) -> tuple[bool, str]:
    project = bot_dir(user_id, bot_id)
    file_path = (project / filename).resolve()

    if not str(file_path).startswith(str(project.resolve())):
        return False, "Access denied"

    if len(content.encode()) > MAX_EDIT_FILE_SIZE:
        return False, "Content too large"

    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
        return True, "Saved"
    except Exception as e:
        return False, str(e)


# ──────────────────────────────────────────────
# System stats
# ──────────────────────────────────────────────
def get_system_stats() -> dict:
    try:
        import psutil
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1)
        used_mb = (mem.total - mem.available) / 1024 / 1024
        total_mb = mem.total / 1024 / 1024
        return {
            "cpu_percent": round(cpu, 1),
            "ram_used_mb": round(used_mb),
            "ram_total_mb": round(total_mb),
            "active_bots": len([p for p in _processes.values() if p.poll() is None]),
        }
    except ImportError:
        return {
            "cpu_percent": 0,
            "ram_used_mb": 0,
            "ram_total_mb": 0,
            "active_bots": len([p for p in _processes.values() if p.poll() is None]),
        }


# ──────────────────────────────────────────────
# Cleanup tasks
# ──────────────────────────────────────────────
async def periodic_cleanup():
    """Background task: clean logs, check zombies."""
    while True:
        await asyncio.sleep(3600)  # every hour
        logger.info("Running periodic cleanup...")
        for user_dir in BOTS_DIR.iterdir():
            if not user_dir.is_dir():
                continue
            for bot_dir_path in user_dir.iterdir():
                if not bot_dir_path.is_dir():
                    continue
                try:
                    bid = int(bot_dir_path.name)
                    uid = int(user_dir.name)
                    clear_old_logs(uid, bid)
                except (ValueError, Exception):
                    pass


async def restore_running_bots():
    """On startup, mark previously 'running' bots as stopped (they died with the host)."""
    bots = db.get_all_active_bots()
    for bot in bots:
        db.update_bot_status(bot["id"], "stopped")
        logger.info("Marked bot %d as stopped (host restarted)", bot["id"])
