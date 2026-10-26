#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$ROOT_DIR/.env" ]] || { echo 'Create the canonical .env before installation.' >&2; exit 2; }
cargo build --manifest-path "$ROOT_DIR/Cargo.toml" --release --locked
mkdir -p "$HOME/.config/systemd/user"
cat > "$HOME/.config/systemd/user/annotations-api.service" <<UNIT
[Unit]
Description=Asodya Annotations API
After=network-online.target
[Service]
Type=simple
WorkingDirectory=$ROOT_DIR
ExecStart=$ROOT_DIR/target/release/annotations-api
Environment=RUST_LOG=info
Restart=on-failure
RestartSec=5
[Install]
WantedBy=default.target
UNIT
systemctl --user daemon-reload
systemctl --user enable annotations-api.service
systemctl --user restart annotations-api.service
systemctl --user is-active annotations-api.service
