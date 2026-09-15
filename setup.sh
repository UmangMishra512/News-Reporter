#!/usr/bin/env bash
# setup.sh — One-command setup for India News Intelligence Platform
# Usage: bash setup.sh

set -e

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   India News Intelligence Platform — Setup               ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── Python version check ────────────────────────────────────
PYTHON=$(python3 --version 2>&1 | awk '{print $2}')
MAJOR=$(echo "$PYTHON" | cut -d. -f1)
MINOR=$(echo "$PYTHON" | cut -d. -f2)
if [ "$MAJOR" -lt 3 ] || ([ "$MAJOR" -eq 3 ] && [ "$MINOR" -lt 10 ]); then
  echo "❌  Python 3.10+ required. Found: $PYTHON"
  exit 1
fi
echo "✅  Python $PYTHON found."

# ── Create virtual environment ──────────────────────────────
if [ ! -d ".venv" ]; then
  echo "📦  Creating virtual environment..."
  python3 -m venv .venv
fi
source .venv/bin/activate
echo "✅  Virtual environment activated."

# ── Install dependencies ────────────────────────────────────
echo "📥  Installing Python dependencies (this may take a few minutes)..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "✅  Dependencies installed."

# ── Sentence-transformers model (pre-download) ──────────────
echo "🤖  Pre-downloading deduplication model (all-MiniLM-L6-v2)..."
python3 -c "
try:
    from sentence_transformers import SentenceTransformer
    SentenceTransformer('all-MiniLM-L6-v2')
    print('✅  Model downloaded.')
except Exception as e:
    print(f'⚠️  Model download skipped: {e}')
"

# ── Copy .env if not exists ─────────────────────────────────
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo ""
  echo "📝  Created .env from .env.example"
  echo "    ⚠️  IMPORTANT: Open .env and fill in your values:"
  echo "       • DISCORD_BOT_TOKEN"
  echo "       • DISCORD_CHANNEL_ID"
  echo "       • GEMINI_API_KEY (or your chosen LLM provider)"
  echo ""
else
  echo "✅  .env already exists."
fi

# ── Create storage dirs ─────────────────────────────────────
mkdir -p storage/logs storage/screenshots
echo "✅  Storage directories created."

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   Setup complete! Next steps:                            ║"
echo "║                                                          ║"
echo "║   1. Edit .env with your Discord + LLM credentials      ║"
echo "║   2. Test:  python main.py --dry-run                     ║"
echo "║   3. Run:   python main.py                               ║"
echo "║                                                          ║"
echo "║   Dashboard: http://localhost:8080  (after starting)     ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
