#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_NAME="La Poste Bot"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

export PLAYWRIGHT_BROWSERS_PATH="$APP_DIR/playwright_browsers"
mkdir -p "$PLAYWRIGHT_BROWSERS_PATH"

"$PYTHON_BIN" -m pip install --upgrade pip
"$PYTHON_BIN" -m pip install -r "$APP_DIR/requirements-build.txt"
"$PYTHON_BIN" -m playwright install chromium

rm -f "$APP_DIR/playwright_browsers.zip"
ditto -c -k --sequesterRsrc --keepParent "$PLAYWRIGHT_BROWSERS_PATH" "$APP_DIR/playwright_browsers.zip"

pyinstaller \
  --noconfirm \
  --clean \
  --windowed \
  --name "$APP_NAME" \
  --osx-bundle-identifier "com.siwak.laposte-bot" \
  --distpath "$APP_DIR/dist" \
  --workpath "$APP_DIR/build" \
  --specpath "$APP_DIR/build_spec" \
  --add-data "$APP_DIR/playwright_browsers.zip:." \
  --collect-all flask \
  --collect-all jinja2 \
  --collect-all werkzeug \
  --collect-all pandas \
  --collect-all numpy \
  --collect-all openpyxl \
  --collect-all playwright \
  --collect-all pyee \
  --collect-all greenlet \
  "$APP_DIR/app_laposte.py"
