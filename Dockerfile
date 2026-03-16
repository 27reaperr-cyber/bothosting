# ──────────────────────────────────────────────
# Telegram Bot Hosting — Dockerfile
# python:3.11-slim, no docker.sock, no DinD
# ──────────────────────────────────────────────
FROM python:3.11-slim

# Metadata
LABEL maintainer="bothost"
LABEL description="Telegram Bot Hosting System"

# Env
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for extra security
RUN groupadd -r bothost && useradd -r -g bothost -d /app bothost

# Working directory
WORKDIR /app

# Install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY bot.py runner.py db.py utils.py ./

# Create required directories
RUN mkdir -p bots data tmp && \
    chown -R bothost:bothost /app

# Switch to non-root user
USER bothost

# Expose nothing (bot uses long polling)
# EXPOSE —

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sqlite3; sqlite3.connect('data/bothost.db').execute('SELECT 1')" || exit 1

# Run
CMD ["python", "bot.py"]
