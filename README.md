# Relay Room

![Relay Room illustrated cover](web/cover.svg)

**English** · [Русский](#русский)

A shared board and focus timer built with Rust, WebSockets and a lightweight browser interface.

> Early implementation. The cover is an illustration, not a screenshot. Rooms currently work across tabs on the same computer.

## Run

Install [Rust and Cargo](https://rustup.rs/), then run `cargo run --release` and open <http://127.0.0.1:3001>. On Windows use `run.bat`; on macOS use `sh run.command`. Open the same room link in a second tab to share cards and the timer. The server binds to localhost, so a link does not work on another person's computer. Stop with Ctrl+C.

Use **New room** to generate a fresh room, add cards, mark them done, delete them, and start or stop the shared 25-minute timer. Language and theme preferences stay in your browser. Rooms persist in `relay-room.sqlite` (or the file specified by `RELAY_DB`). The local database contains room contents and should be kept private. The current version supports up to 100 active or saved rooms; a new empty room is saved on its first successful action. There is no login or permission control; anyone with the local room link can edit it. Do not store sensitive information here.

## Русский

Общая доска и таймер для совместной работы: Rust, WebSocket и лёгкий интерфейс в браузере.

> Ранняя версия. Обложка — иллюстрация. Сейчас комнаты работают между вкладками на одном компьютере.

Установите [Rust и Cargo](https://rustup.rs/), выполните `cargo run --release` и откройте <http://127.0.0.1:3001>. В Windows можно запустить `run.bat`, на macOS — `sh run.command`. Откройте ссылку комнаты во второй вкладке: карточки и таймер общие. Сервер работает только на локальном компьютере — на другом устройстве ссылка пока не откроется. Остановка — Ctrl+C.

Создавайте комнату, добавляйте и отмечайте карточки, запускайте общий таймер на 25 минут. Язык и тема сохраняются в браузере. Комнаты сохраняются в `relay-room.sqlite` (путь можно задать через `RELAY_DB`); файл содержит данные комнат, храните его как личный. Поддерживается до 100 активных или сохранённых комнат; новая пустая комната записывается при первом успешном действии. Авторизации пока нет; любой человек с локальной ссылкой может менять доску. Не храните в ней конфиденциальные данные.

### Restore deleted cards / Возврат карточки
After server-confirmed deletion, Restore is available for up to 30 seconds. It restores only that card; repeated restores cannot duplicate it. The transient undo record is scoped to the room and is lost on server restart. Storage failure is reported instead of silently claiming success. Existing shared-local-link permissions remain unchanged.
После подтверждённого удаления карточку можно вернуть до 30 секунд. Повтор не создаёт копию и не затирает другие изменения. История возврата не переживает перезапуск сервера; права по локальной ссылке остаются прежними.
Startup diagnostics: `cargo run --release -- --check` checks the database location and local port without starting a server. / Проверка запуска без сервера: `--check`.

### Confirmed saving / Подтверждённое сохранение
The board now shows Ready, Saving, Saved, Not saved and Confirmation pending separately from connection status. New card text remains in the field until its action is acknowledged; if you keep typing, a later confirmation does not erase the newer draft. Reconnect checks the same pending command instead of creating a second action. Conflicts show the current committed board and ask you to review it.

Панель различает готовность, сохранение, подтверждение, отказ и неизвестный результат. Текст новой карточки остаётся в поле до подтверждения; новый текст, введённый во время ожидания, не стирается. После переподключения проверяется та же команда. При конфликте показывается действующая доска и предлагается повторить действие после проверки.

SQLite runs on a dedicated thread with a bounded queue. Each room owns its command queue and publishes only committed snapshots. Room lookup, existing snapshots and the health endpoint do not wait under a global database lock. SQLite writes themselves are still serialized.

Modern commands carry request_id and expected_revision. The last 128 successful receipts are stored with the snapshot (fingerprints only, not a second copy of card text). Exact replays return the recorded result, including after restart. If a receipt was evicted, its old revision is rejected rather than applied again. Legacy messages remain accepted but have no general retry guarantee. The transient 30-second restore window still does not survive restart.

Современные команды используют request_id и expected_revision. Последние 128 успешных подтверждений сохраняются вместе со снимком; повтор не создаёт второе действие даже после перезапуска. Если запись подтверждения уже вытеснена, устаревшая версия отклоняется. Старые сообщения поддерживаются без общей гарантии безопасного повтора. Окно возврата удалённой карточки по-прежнему не переживает перезапуск.

Limits: 128 connections, 16 queued commands per room, 32 queued SQLite writes, 4 KiB incoming messages, a two-second queue admission budget, up to 750 ms SQLite lock wait and a three-second socket-send budget. A stalled filesystem call cannot be forcibly interrupted by these budgets. Unknown storage state blocks further mutation of that room until server restart; never assume an unconfirmed action failed and blindly submit a new copy. Pending commands and drafts stay in the current tab, not across a full page reload.
