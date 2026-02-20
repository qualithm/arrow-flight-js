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

## API Reference

### Connection

```typescript
import { FlightClient, createFlightClient } from "@qualithm/arrow-flight-js"

// Option 1: Create and connect separately
const client = new FlightClient({ host: "localhost", port: 8815 })
await client.connect()

// Option 2: Create and connect in one step
const client = await createFlightClient({ host: "localhost", port: 8815 })

// Close when done
client.close()
```

### Authentication

```typescript
// Basic auth
const client = await createFlightClient({
  host: "localhost",
  auth: { type: "basic", username: "user", password: "pass" }
})

// Bearer token
const client = await createFlightClient({
  host: "localhost",
  auth: { type: "bearer", token: "your-token" }
})

// mTLS
const client = await createFlightClient({
  host: "localhost",
  auth: {
    type: "mtls",
    cert: fs.readFileSync("client.crt"),
    key: fs.readFileSync("client.key"),
    ca: fs.readFileSync("ca.crt")
  }
})

// Flight Handshake
await client.connect()
const result = await client.handshake()
console.log("Token:", result.token)
```

### TLS Configuration

```typescript
const client = await createFlightClient({
  host: "flight.example.com",
  port: 443,
  tls: {
    rootCerts: fs.readFileSync("ca.crt"),
    // For mTLS:
    certChain: fs.readFileSync("client.crt"),
    privateKey: fs.readFileSync("client.key"),
    // Override server name for certificate verification
    serverNameOverride: "flight.example.com"
  }
})
```

### Descriptors

Flights are identified by descriptors—either path-based or command-based:

```typescript
import { pathDescriptor, cmdDescriptor } from "@qualithm/arrow-flight-js"

// Path-based: identifies a dataset by path segments
const pathDesc = pathDescriptor("database", "schema", "table")

// Command-based: contains an opaque command
const cmdDesc = cmdDescriptor(Buffer.from("SELECT * FROM users"))
```

### Read Operations

```typescript
// List available flights
for await (const info of client.listFlights()) {
  console.log(info.flightDescriptor)
}

// Get info about a specific flight
const info = await client.getFlightInfo(pathDescriptor("my", "data"))

// Get just the schema
const schema = await client.getSchema(pathDescriptor("my", "data"))

// Retrieve data (DoGet)
for (const endpoint of info.endpoint) {
  for await (const data of client.doGet(endpoint.ticket!)) {
    // data.dataHeader contains Arrow IPC message header
    // data.dataBody contains the Arrow record batch
  }
}
```

### Write Operations

```typescript
// Upload data (DoPut)
const stream = client.doPut()

// Send descriptor with first message
stream.write({
  flightDescriptor: { type: 1, path: ["my", "table"], cmd: Buffer.alloc(0) },
  dataHeader: schemaBytes,
  appMetadata: Buffer.alloc(0),
  dataBody: Buffer.alloc(0)
})

// Send data batches
for (const batch of batches) {
  stream.write({
    flightDescriptor: undefined,
    dataHeader: batch.header,
    appMetadata: Buffer.alloc(0),
    dataBody: batch.body
  })
}

// End and collect acknowledgements
stream.end()
const results = await stream.collectResults()

// Bidirectional exchange (DoExchange)
const exchange = client.doExchange()
exchange.write(flightData)
for await (const result of exchange.results()) {
  console.log("Received:", result)
}
exchange.end()
```

### Actions

```typescript
// Execute a custom action
const action = { type: "clear-cache", body: Buffer.alloc(0) }
for await (const result of client.doAction(action)) {
  console.log("Result:", result.body.toString())
}

// List available actions
for await (const actionType of client.listActions()) {
  console.log(`${actionType.type}: ${actionType.description}`)
}
```

### Error Handling

```typescript
import { FlightError } from "@qualithm/arrow-flight-js"

try {
  await client.getFlightInfo(pathDescriptor("unknown"))
} catch (error) {
  if (FlightError.isNotFound(error)) {
    console.log("Flight not found")
  } else if (FlightError.isUnauthenticated(error)) {
    console.log("Authentication required")
  } else if (FlightError.isPermissionDenied(error)) {
    console.log("Permission denied")
  } else if (FlightError.isRetriable(error)) {
    console.log("Transient error, retry...")
  } else {
    throw error
  }
}

// Access error details
if (error instanceof FlightError) {
  console.log("Code:", error.code) // e.g., "NOT_FOUND"
  console.log("gRPC code:", error.grpcCode) // e.g., 5
  console.log("Details:", error.details)
  console.log("Metadata:", error.metadata)
}
```

### Call Options

All RPC methods accept optional call options:

```typescript
const info = await client.getFlightInfo(descriptor, {
  timeoutMs: 5000,
  headers: { "x-request-id": "abc123" }
})
```

## Supported Methods

| Method            | Description                              |
| ----------------- | ---------------------------------------- |
| `handshake()`     | Authentication handshake                 |
| `listFlights()`   | List available data streams              |
| `getFlightInfo()` | Get metadata about a specific flight     |
| `getSchema()`     | Get the Arrow schema for a flight        |
| `doGet()`         | Retrieve a data stream (server → client) |
| `doPut()`         | Upload a data stream (client → server)   |
| `doExchange()`    | Bidirectional data stream exchange       |
| `doAction()`      | Execute a custom action                  |
| `listActions()`   | List available custom actions            |

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
bun test
```

### Linting & Formatting

```bash
bun run lint
bun run format
bun run typecheck
```

## License

Apache-2.0
