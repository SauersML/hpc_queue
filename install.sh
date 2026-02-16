#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${Q_REPO_URL:-https://github.com/SauersML/hpc_queue.git}"
REPO_BRANCH="${Q_REPO_BRANCH:-main}"
INSTALL_DIR="${Q_INSTALL_DIR:-$HOME/.local/share/hpc_queue}"
BIN_DIR="${Q_BIN_DIR:-$HOME/.local/bin}"
Q_LINK="$BIN_DIR/q"
PATH_LINE='export PATH="$HOME/.local/bin:$PATH"'

is_sourced=0
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  is_sourced=1
fi

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

append_once() {
  local file="$1"
  local line="$2"
  mkdir -p "$(dirname "$file")"
  touch "$file"
  if ! grep -Fq "$line" "$file"; then
    printf '\n%s\n' "$line" >>"$file"
  fi
}

echo "Installing q..."
need_cmd git
need_cmd python3

mkdir -p "$(dirname "$INSTALL_DIR")"
if [ -d "$INSTALL_DIR/.git" ]; then
  echo "Updating existing install at $INSTALL_DIR"
  git -C "$INSTALL_DIR" fetch --depth=1 origin "$REPO_BRANCH"
  git -C "$INSTALL_DIR" checkout -f "$REPO_BRANCH"
  git -C "$INSTALL_DIR" reset --hard "origin/$REPO_BRANCH"
else
  echo "Cloning $REPO_URL into $INSTALL_DIR"
  rm -rf "$INSTALL_DIR"
  git clone --depth=1 --branch "$REPO_BRANCH" "$REPO_URL" "$INSTALL_DIR"
fi

chmod +x "$INSTALL_DIR/q.py"
mkdir -p "$BIN_DIR"
ln -sf "$INSTALL_DIR/q.py" "$Q_LINK"
chmod +x "$Q_LINK"

append_once "$HOME/.zshrc" "$PATH_LINE"
append_once "$HOME/.bashrc" "$PATH_LINE"
append_once "$HOME/.bash_profile" "$PATH_LINE"
append_once "$HOME/.profile" "$PATH_LINE"

if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
  export PATH="$BIN_DIR:$PATH"
fi

echo
echo "q installed:"
echo "  $Q_LINK -> $INSTALL_DIR/q.py"
echo
echo "Run:"
echo "  q --help"
echo "  q login"

if [[ "$is_sourced" -eq 0 && -t 1 && -z "${Q_INSTALL_NO_REEXEC:-}" ]]; then
  echo
  echo "Opening a fresh login shell so q is available immediately..."
  exec "${SHELL:-/bin/bash}" -l
fi
