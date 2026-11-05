# Project notes

Localhost Axum/WebSocket application. The room registry only contains Arc handles and never performs SQL. Each room is an independent bounded actor that owns mutable data; clients and the watch/broadcast channels see committed snapshots only. A dedicated OS thread owns SQLite, using WAL and synchronous=FULL; writes are serialized through a 32-slot channel. Room commands use a 16-slot channel and a two-second admission deadline. This removes synchronous SQLite from Tokio workers and the shared room-registry mutex, not SQLite's single-writer constraint.

## Command contract

Modern messages add request_id and expected_revision to the existing flat command. A matching saved receipt returns success without reapplying the command. Reusing an ID with different data is rejected. A missing receipt plus an old expected revision is a conflict, including after eviction. Up to 128 receipts are persisted inside the existing rooms.snapshot JSON, atomically with the board and next_id. They store SHA-256 command fingerprints, not plaintext copies of deleted notes. No permission or ownership is inferred from a request ID.

Candidate changes, next_id, receipts and transient restore records are cloned before mutation. A storage rejection leaves the published room untouched. If a write reports an error, the worker reads its final saved row: an exact match is confirmed, a known old row is rejected, an unreadable outcome marks the room uncertain. Such a room rejects mutations until restart rather than making an unsafe guess. A detached client cannot cancel the room actor's accepted command.

Snapshots add a defaulted revision field. Old rows load at revision zero; new empty rooms need no database write until their first valid action. Stored data is validated with existing 100-card/200-character limits and safe integer IDs. Legacy messages remain accepted without modern retry guarantees. Deleted-card restore records are bounded, room-scoped, valid for 30 seconds and intentionally transient across restart.

## Interface

A theme-aware save panel distinguishes connection, pending, durable confirmation, conflict and unknown outcome. Draft text clears only when its own confirmed submission still matches the field. One local command is pending at a time. Reconnect retries its original envelope after receiving the current snapshot. A deliberate room change never resends an old room's command into a different room. Keyed card updates preserve unaffected DOM nodes and focus. Text is inserted with textContent; EN/RU, custom theme controls, thin themed scrollbars and reduced-motion remain supported.

## Boundaries and tests

128 connections, 100 active/saved rooms, bounded queues/broadcast/write buffers, 4 KiB inbound frames/messages, a 750 ms maximum SQLite lock wait and a three-second socket-send budget. Queue deadlines do not forcibly preempt stalled filesystem calls. Recent operations and their expected versions survive restart; a browser tab's pending state does not survive full reload. Durability is SQLite commit, not a promise against every hardware fault.

Rust tests exercise independent room progress, no early publication, failed save, detached waiter, bounded queues and receipt eviction. Linux tests use real WebSockets and SQLite: legacy restore, storage trigger failure, health/room lookup during a database lock, lost response, restart and safe replay. Physical Windows/macOS launchers and manual screen-reader testing are distinct from CI builds/tests.
