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

**Key capabilities:**

- All core Flight RPC methods (Handshake, ListFlights, GetFlightInfo, GetSchema, DoGet, DoPut,
  DoExchange, DoAction, ListActions)
- Connection management and authentication (Bearer tokens, mTLS, Flight Handshake)
- Arrow IPC stream encoding/decoding
- Support for Bun, Node.js, and browser runtimes

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
| `client.ts` | FlightClient connection manager |
| `index.ts`  | Main entry point                |
| `types.ts`  | Flight protocol types           |

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

### Risks

| ID  | Risk                        | Impact | Mitigation                            |
| --- | --------------------------- | ------ | ------------------------------------- |
| R-1 | gRPC in browsers is complex | High   | Consider Connect protocol as fallback |
| R-2 | Large bundle size           | Medium | Tree-shaking, optional heavy deps     |

---

## Work In Flight

> Claim work before starting. Include start timestamp. Remove within 24 hours of completion.

| ID  | Agent | Started | Task | Files |
| --- | ----- | ------- | ---- | ----- |
| —   | —     | —       | None | —     |

---

## Work Queue

> No items currently queued.

---

## Learnings

> Append-only. Never edit or delete existing entries.

| Date       | Learning                                                                                                                                             |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2025-07-18 | FlightError constructor takes `(message, code)` not `(code, message)` — message first for compatibility with standard Error class semantics          |
| 2026-02-24 | FlightError accepts Buffer metadata from gRPC errors (not just strings) — check for Buffer type when extracting error details                        |
| 2026-02-24 | Mock gRPC streams using EventEmitter + vi.mock on grpcClient property — enables testing stream error handling without real server                    |
| 2026-02-24 | To test checkServerIdentity callback: use vi.hoisted to capture verifyOptions before module loads, then call connect() to trigger buildCredentials() |
