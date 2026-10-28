@echo off
cd /d "%~dp0"
where cargo >nul 2>&1 || (echo Install Rust from https://rustup.rs/ & exit /b 1)
start "" http://127.0.0.1:3001
cargo run --release
pause
