# Arrow Flight JS

Arrow Flight client for JavaScript and TypeScript runtimes.

A high-performance transport layer for Apache Arrow data streams using gRPC. This is the **base
protocol** library—for SQL functionality, see
[`@qualithm/arrow-flight-sql-js`](https://github.com/qualithm/arrow-flight-sql-js).

## Features

- Full Arrow Flight protocol support (all RPC methods)
- TypeScript-first with comprehensive type definitions
- Streaming support with async iterables
- Multiple authentication methods (Basic, Bearer, mTLS, Handshake)
- TLS/mTLS configuration
- Comprehensive error handling with typed error codes
- ESM-only, tree-shakeable

## Installation

```bash
npm install @qualithm/arrow-flight-js
# or
bun add @qualithm/arrow-flight-js
```

## Quick Start

```typescript
import { createFlightClient, pathDescriptor } from "@qualithm/arrow-flight-js"

// Connect to a Flight server
const client = await createFlightClient({
  host: "localhost",
  port: 8815,
  tls: false
})

// List available flights
for await (const info of client.listFlights()) {
  console.log("Flight:", info.flightDescriptor)
}

// Get flight info
const info = await client.getFlightInfo(pathDescriptor("my", "dataset"))
console.log("Records:", info.totalRecords)

// Retrieve data
for (const endpoint of info.endpoint) {
  for await (const data of client.doGet(endpoint.ticket!)) {
    console.log("Received:", data.dataBody.length, "bytes")
  }
}

client.close()
```

See the [examples](./examples) directory for complete, runnable demonstrations.

## Examples

| Example                                                 | Description                                       |
| ------------------------------------------------------- | ------------------------------------------------- |
| [basic-connection.ts](./examples/basic-connection.ts)   | Creating clients, connecting, listing flights     |
| [authentication.ts](./examples/authentication.ts)       | Basic auth, bearer tokens, handshake              |
| [tls-configuration.ts](./examples/tls-configuration.ts) | TLS, mTLS, custom CAs, server name override       |
| [getting-data.ts](./examples/getting-data.ts)           | `getFlightInfo()`, `getSchema()`, `doGet()`       |
| [putting-data.ts](./examples/putting-data.ts)           | Upload data with `doPut()`                        |
| [exchange.ts](./examples/exchange.ts)                   | Bidirectional streaming with `doExchange()`       |
| [actions.ts](./examples/actions.ts)                     | Custom actions with `listActions()`, `doAction()` |
| [streaming.ts](./examples/streaming.ts)                 | Memory-efficient streaming patterns               |
| [cancellation.ts](./examples/cancellation.ts)           | Cancel flights with `cancelFlightInfo()`          |
| [call-options.ts](./examples/call-options.ts)           | Timeouts, custom headers, request tracing         |
| [error-handling.ts](./examples/error-handling.ts)       | `FlightError` handling patterns                   |

## API Reference

### Client Methods

| Method        | Description                        |
| ------------- | ---------------------------------- |
| `connect()`   | Establish connection to the server |
| `close()`     | Close the connection               |
| `handshake()` | Authentication handshake           |

### Flight Operations

| Method            | Description                              |
| ----------------- | ---------------------------------------- |
| `listFlights()`   | List available data streams              |
| `getFlightInfo()` | Get metadata about a specific flight     |
| `getSchema()`     | Get the Arrow schema for a flight        |
| `doGet()`         | Retrieve a data stream (server → client) |
| `doPut()`         | Upload a data stream (client → server)   |
| `doExchange()`    | Bidirectional data stream exchange       |

### Actions

| Method          | Description                   |
| --------------- | ----------------------------- |
| `doAction()`    | Execute a custom action       |
| `listActions()` | List available custom actions |

### Cancellation

| Method               | Description             |
| -------------------- | ----------------------- |
| `cancelFlightInfo()` | Cancel a running flight |

### Utilities

| Function           | Description                              |
| ------------------ | ---------------------------------------- |
| `pathDescriptor()` | Create a path-based flight descriptor    |
| `cmdDescriptor()`  | Create a command-based flight descriptor |

## Development

### Prerequisites

- [Bun](https://bun.sh/) (recommended) or Node.js 20+

### Setup

```bash
bun install
```

### Building

```bash
bun run build
```

### Testing

```bash
# Unit tests
bun test

# Integration tests (requires running Arrow Flight server)
FLIGHT_HOST=localhost FLIGHT_PORT=50051 bun run test:integration
```

### Linting & Formatting

```bash
bun run lint
bun run format
bun run typecheck
```

## License

Apache-2.0
