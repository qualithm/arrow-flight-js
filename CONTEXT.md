# CONTEXT.md

> **This is the single source of truth for this repository.**
>
> When CONTEXT.md conflicts with any other document (README, code comments), CONTEXT.md is correct.
> Update other documents to match, not the reverse.

---

## System Intent

Arrow Flight client for JavaScript and TypeScript runtimes.

Provides a high-performance transport layer for Apache Arrow data streams using gRPC. This is the
**base protocol** library—it does NOT include SQL-specific functionality (see `arrow-flight-sql-js`
for that).

---

## Current Reality

### Architecture

| Component | Technology             |
| --------- | ---------------------- |
| Language  | TypeScript (ESM-only)  |
| Runtime   | Bun, Node.js 20+, Deno |
| Build     | TypeScript compiler    |
| Proto     | ts-proto + grpc-tools  |
| Test      | Vitest                 |
| Lint      | ESLint, Prettier       |
| Docs      | TypeDoc                |

### Modules

| Module      | Purpose                         |
| ----------- | ------------------------------- |
| `index.ts`  | Main entry point                |
| `types.ts`  | Flight protocol types           |
| `client.ts` | FlightClient connection manager |

### Core Arrow Flight RPC Methods

| Method             | Purpose                                  |
| ------------------ | ---------------------------------------- |
| `Handshake`        | Authentication handshake                 |
| `ListFlights`      | List available data streams              |
| `GetFlightInfo`    | Get metadata about a specific flight     |
| `GetSchema`        | Get the Arrow schema for a flight        |
| `DoGet`            | Retrieve a data stream (server → client) |
| `DoPut`            | Upload a data stream (client → server)   |
| `DoExchange`       | Bidirectional data stream exchange       |
| `DoAction`         | Execute a custom action, return results  |
| `ListActions`      | List available custom actions            |
| `CancelFlightInfo` | Cancel a running flight operation        |

---

## Scope

### In Scope

- Arrow Flight client implementation (gRPC-based)
- All core Flight RPC methods listed above
- Connection management and authentication
- Arrow IPC stream encoding/decoding
- Support for Bun, Node.js, and browser runtimes
- TLS/mTLS support

### Out of Scope

- SQL query execution (use `arrow-flight-sql-js`)
- Prepared statements (use `arrow-flight-sql-js`)
- Database metadata queries (use `arrow-flight-sql-js`)
- Server implementation (client-only library)

---

## Locked Decisions

1. **Client-only** — No server implementation; focus on client consumption
2. **TypeScript-first** — Type safety for all code
3. **ESM-only** — Modern standards, tree-shaking
4. **Minimal runtime deps** — Bundle size, supply chain risk
5. **Apache Arrow JS integration** — Use official `apache-arrow` package
6. **gRPC library: `@grpc/grpc-js`** — Official Google library, battle-tested, Node.js native
7. **Browser support: gRPC-Web** — Requires Envoy proxy, widely supported by Flight servers
8. **Authentication: All patterns** — Bearer tokens, mTLS, and Flight Handshake for maximum
   compatibility
9. **Proto handling: Vendored** — Flight.proto from apache/arrow vendored in `proto/` for
   reproducible builds; compiled with ts-proto

---

## Open Decisions & Risks

### Open Decisions

| ID  | Question | Context |
| --- | -------- | ------- |
| —   | None     | —       |

### Risks

| ID  | Risk                        | Impact | Mitigation                            |
| --- | --------------------------- | ------ | ------------------------------------- |
| R1  | gRPC in browsers is complex | High   | Consider Connect protocol as fallback |
| R2  | Large bundle size           | Medium | Tree-shaking, optional heavy deps     |

---

## Work In Flight

> Claim work before starting. Include start timestamp. Remove within 24 hours of completion.

| ID  | Agent | Started | Task | Files |
| --- | ----- | ------- | ---- | ----- |
|     |       |         |      |       |

---

## Next Milestones

### M1: Project Setup

- [x] Update package.json with correct name/description
- [x] Define proto file handling strategy
- [x] Set up proto compilation pipeline
- [x] Add core dependencies (`apache-arrow`)

### M2: Core Types & Connection

- [x] Define TypeScript types for Flight protocol
- [x] Implement `FlightClient` connection management
- [x] Implement `Handshake` for authentication
- [x] Add TLS configuration support

### M3: Read Operations

- [x] Implement `ListFlights`
- [x] Implement `GetFlightInfo`
- [x] Implement `GetSchema`
- [x] Implement `DoGet` with Arrow IPC decoding

### M4: Write Operations

- [x] Implement `DoPut` with Arrow IPC encoding
- [x] Implement `DoExchange` for bidirectional streams

### M5: Actions & Polish

- [x] Implement `DoAction`
- [x] Implement `ListActions`
- [x] Add comprehensive error handling
- [x] Documentation and examples

### M6: Testing Infrastructure

- [x] Add test connection configuration for Arrow Flight server
- [x] Create integration test suite (listFlights, getFlightInfo, doGet, doPut)
- [x] Run tests on Bun (unit + integration)
- [x] Run tests on Node.js (unit + integration)
- [x] Run tests on Deno (unit + integration)
- [x] Enable coverage reporting with threshold enforcement
- [x] Add test fixtures for Arrow schemas/data (uses test server fixtures)

### M7: Additional Flight Operations

- [x] Implement `cancelFlightInfo` method (CancelFlightInfo action) — needed by arrow-flight-sql-js
- [x] Export `CancelFlightInfoRequest` / `CancelFlightInfoResult` encoders from generated proto

---

## Learnings

> Append-only. Never edit or delete existing entries.

| Date       | Learning                                                                                                                                        |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| 2025-07-18 | FlightError constructor takes `(message, code)` not `(code, message)` — message first for compatibility with standard Error class semantics     |
| 2026-02-23 | Integration tests target Arrow Flight server; config via env vars FLIGHT_HOST/FLIGHT_PORT; tests cover connection, flights, data, actions, auth |
