# Examples

This directory contains example scripts demonstrating common use cases for the Arrow Flight JS
client.

## Prerequisites

1. A running Arrow Flight server
2. Install dependencies: `bun install`

## Running Examples

```bash
# Basic connection and flight listing
bun run examples/basic-connection.ts

# Authentication and TLS configuration
bun run examples/authentication.ts

# Retrieving data (includes call options and cancellation)
bun run examples/getting-data.ts

# Uploading data with doPut
bun run examples/putting-data.ts

# Custom actions
bun run examples/actions.ts

# Error handling patterns
bun run examples/error-handling.ts

# Arrow IPC data serialisation
bun run examples/arrow-ipc.ts

# Browser / gRPC-Web (documentation only)
# See examples/browser-grpc-web.ts
```

## Example Overview

### [basic-connection.ts](basic-connection.ts)

Connecting to a Flight server and listing available flights. Demonstrates:

- Creating a FlightClient
- Using `createFlightClient()` for one-step connection
- Listing flights with `listFlights()`
- Closing the connection

### [authentication.ts](authentication.ts)

Authentication methods and TLS configuration. Demonstrates:

- No authentication (local development)
- Basic authentication (username/password)
- Bearer token authentication (JWT/OAuth)
- Handshake authentication
- Raw handshake payload for custom protocols
- Default TLS with system certificates
- Custom root CA certificates
- Mutual TLS (mTLS) with client certificates

### [getting-data.ts](getting-data.ts)

Retrieving data from a Flight server. Demonstrates:

- Getting flight metadata with `getFlightInfo()`
- Getting schema with `getSchema()`
- Retrieving data with `doGet()`
- Using path and command descriptors
- Parsing endpoint locations with `parseLocation()`
- Creating locations with `createLocation()`
- Setting timeouts and custom headers (call options)
- Stream cancellation

### [putting-data.ts](putting-data.ts)

Uploading data to a Flight server. Demonstrates:

- Creating a DoPut stream
- Sending flight descriptor and schema
- Uploading data batches
- Collecting put acknowledgements

### [actions.ts](actions.ts)

Executing custom server actions. Demonstrates:

- Listing available actions with `listActions()`
- Executing actions with `doAction()`
- Processing action results

### [error-handling.ts](error-handling.ts)

Handling various error types from Flight operations. Demonstrates:

- Catching FlightError exceptions
- Checking error codes and categories
- Using error helper methods (isNotFound, isRetriable, etc.)
- Accessing error details and metadata

### [arrow-ipc.ts](arrow-ipc.ts)

Working with Apache Arrow data in IPC format. Demonstrates:

- Creating Arrow RecordBatch from JavaScript arrays
- Serialising data to Arrow IPC stream format
- Parsing IPC data received from Flight
- Full round-trip with doPut (uploading Arrow data)
- Full round-trip with doGet (fetching and parsing Arrow data)

### [browser-grpc-web.ts](browser-grpc-web.ts)

Browser integration using gRPC-Web (documentation only). Covers:

- Architecture overview (Browser → Envoy → Flight Server)
- Envoy proxy configuration for gRPC-Web
- Browser client setup with @grpc/grpc-web
- React component example for fetching Arrow data
- Authentication in browser environments
- Limitations of gRPC-Web (streaming, performance)

## Configuration

Most examples connect to `localhost:8815` by default. Modify the connection options to match your
server:

```typescript
const client = await createFlightClient({
  host: "your-server.example.com",
  port: 443,
  tls: true,
  auth: { type: "bearer", token: "your-token" }
})
```
