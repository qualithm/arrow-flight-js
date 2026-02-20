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

# Authentication patterns
bun run examples/authentication.ts

# TLS and mTLS configuration
bun run examples/tls-configuration.ts

# Retrieving data with doGet
bun run examples/getting-data.ts

# Uploading data with doPut
bun run examples/putting-data.ts

# Bidirectional data exchange
bun run examples/exchange.ts

# Custom actions
bun run examples/actions.ts

# Streaming large datasets
bun run examples/streaming.ts

# Query cancellation
bun run examples/cancellation.ts

# Call options (timeouts, headers)
bun run examples/call-options.ts

# Error handling patterns
bun run examples/error-handling.ts
```

## Example Overview

### [basic-connection.ts](basic-connection.ts)

Connecting to a Flight server and listing available flights. Demonstrates:

- Creating a FlightClient
- Using `createFlightClient()` for one-step connection
- Listing flights with `listFlights()`
- Closing the connection

### [authentication.ts](authentication.ts)

Different authentication methods for connecting. Demonstrates:

- No authentication (local development)
- Basic authentication (username/password)
- Bearer token authentication (JWT/OAuth)
- Handshake authentication
- Two-step connection pattern
- Raw handshake payload for custom protocols

### [tls-configuration.ts](tls-configuration.ts)

Secure connections with TLS and mTLS. Demonstrates:

- Default TLS with system certificates
- Custom root CA certificates
- Mutual TLS (mTLS) with client certificates
- Server name override
- Skipping certificate verification (development only)
- Custom gRPC credentials for advanced scenarios
- Insecure connections (development only)

### [getting-data.ts](getting-data.ts)

Retrieving data from a Flight server. Demonstrates:

- Getting flight metadata with `getFlightInfo()`
- Getting schema with `getSchema()`
- Retrieving data with `doGet()`
- Using path and command descriptors
- Parsing endpoint locations with `parseLocation()`
- Creating locations with `createLocation()`

### [putting-data.ts](putting-data.ts)

Uploading data to a Flight server. Demonstrates:

- Creating a DoPut stream
- Sending flight descriptor and schema
- Uploading data batches
- Collecting put acknowledgements

### [exchange.ts](exchange.ts)

Bidirectional data exchange with a Flight server. Demonstrates:

- Creating a DoExchange stream
- Sending data while receiving results
- Processing streaming responses
- Completing the exchange

### [actions.ts](actions.ts)

Executing custom server actions. Demonstrates:

- Listing available actions with `listActions()`
- Executing actions with `doAction()`
- Processing action results

### [streaming.ts](streaming.ts)

Processing large result sets without loading everything into memory. Demonstrates:

- Streaming flight data with async iterables
- Processing batches incrementally
- Memory-efficient data handling

### [cancellation.ts](cancellation.ts)

Cancelling running flight operations. Demonstrates:

- Getting flight info for a long-running operation
- Cancelling with `cancelFlightInfo()`
- Stream cancellation with `stream.cancel()`
- Handling cancellation results

### [call-options.ts](call-options.ts)

Configuring RPC call options. Demonstrates:

- Setting timeouts with `timeoutMs`
- Adding custom headers
- Request tracing with correlation IDs
- Combining multiple options
- Configuring channel options (keepalive, max message size, connection timeout)

### [error-handling.ts](error-handling.ts)

Handling various error types from Flight operations. Demonstrates:

- Catching FlightError exceptions
- Checking error codes and categories
- Using error helper methods (isNotFound, isRetriable, etc.)
- Accessing error details and metadata
