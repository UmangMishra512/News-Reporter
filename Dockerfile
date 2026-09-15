FROM python:3.11-slim

LABEL maintainer="India News Intelligence Platform"
LABEL description="Production-grade multi-agent AI news platform"

# System dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright + Chromium (optional browser automation)
RUN playwright install chromium --with-deps || true

# Copy application code
COPY . .

# Create storage directories
RUN mkdir -p storage/logs storage/screenshots

# Environment defaults
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV DATABASE_PATH=/app/storage/news.db
ENV LOG_DIR=/app/storage/logs

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import aiosqlite; import asyncio; asyncio.run(aiosqlite.connect('storage/news.db'))" || exit 1

EXPOSE 8080

VOLUME ["/app/storage"]

CMD ["python", "main.py"]
