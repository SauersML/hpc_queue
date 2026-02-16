#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/SauersML/hpc_queue.git"
REPO_BRANCH="main"
INSTALL_DIR="$HOME/.local/share/hpc_queue"
BIN_DIR="$HOME/.local/bin"
Q_LINK="$BIN_DIR/q"
PATH_LINE='export PATH="$HOME/.local/bin:$PATH"'

is_sourced=0
if (return 0 2>/dev/null); then
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
# Legacy installs created q as a symlink to q.py.
# If that symlink still exists, writing launcher content would overwrite q.py.
if [[ -L "$Q_LINK" ]]; then
  rm -f "$Q_LINK"
fi
cat >"$Q_LINK" <<EOF
#!/usr/bin/env bash
set -euo pipefail

Q_ROOT="$INSTALL_DIR"
Q_PY="\$Q_ROOT/q.py"

supports_python() {
  local py="\$1"
  "\$py" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
}

pick_python() {
  local candidates=(python3.12 python3.11 python3.10 python3)
  local c
  for c in "\${candidates[@]}"; do
    if command -v "\$c" >/dev/null 2>&1; then
      local p
      p="\$(command -v "\$c")"
      if supports_python "\$p"; then
        echo "\$p"
        return 0
      fi
    fi
  done

  if command -v module >/dev/null 2>&1; then
    local mods=(
      "python3/3.12.4_anaconda2024.06-1_libmamba"
      "python3/3.10.9_anaconda2023.03_libmamba"
      "python/3.10.9_anaconda2023.03_libmamba"
    )
    local m
    for m in "\${mods[@]}"; do
      if module load "\$m" >/dev/null 2>&1; then
        for c in "\${candidates[@]}"; do
          if command -v "\$c" >/dev/null 2>&1; then
            local p2
            p2="\$(command -v "\$c")"
            if supports_python "\$p2"; then
              echo "\$p2"
              return 0
            fi
          fi
        done
      fi
    done
  fi
  return 1
}

PYTHON_BIN="\$(pick_python || true)"
if [[ -z "\$PYTHON_BIN" ]]; then
  echo "q requires Python 3.10+." >&2
  echo "Set one of these up, then re-run q:" >&2
  echo "  module load python3/3.10.9_anaconda2023.03_libmamba" >&2
  exit 1
fi

export PYTHON_BIN
exec "\$PYTHON_BIN" "\$Q_PY" "\$@"
EOF
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
echo "  $Q_LINK (launcher for $INSTALL_DIR/q.py)"
echo
echo "Run:"
echo "  q --help"
echo "  q login"

if [[ "$is_sourced" -eq 0 && -t 1 ]]; then
  echo
  echo "Opening a fresh login shell so q is available immediately..."
  exec "${SHELL:-/bin/bash}" -l
fi
