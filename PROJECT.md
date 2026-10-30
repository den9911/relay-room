# Project notes

Each room is identified by a random URL fragment and keeps up to 100 cards and a shared timer in memory and persists each accepted change to SQLite. Axum validates WebSocket messages, updates the authoritative state and broadcasts snapshots through bounded channels. A client that falls behind receives a fresh snapshot. Clients reconnect automatically after a lost connection. The local server limits room IDs and total room count; server restart restores rooms from the database.

The server binds to loopback and has no accounts or access control. It is not an internet deployment. Future work: authenticated multi-user hosting, room cleanup and browser-based integration tests against the real Rust server.
