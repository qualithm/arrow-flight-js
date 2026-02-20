#!/usr/bin/env bun
/**
 * Demo script showcasing arrow-flight-js.
 *
 * Run with: bun run demo
 */

import {
  cmdDescriptor,
  FlightClient,
  FlightError,
  fromDescriptorTypeProto,
  pathDescriptor,
  toFlightDescriptor
} from "../src/index.js"

console.log("Arrow Flight JS Demo")
console.log("====================\n")

// -------------------- Descriptors --------------------

console.log("--- Descriptors ---\n")

// Create a path-based descriptor (identifies a dataset by path)
const pathDesc = pathDescriptor("database", "schema", "table")
console.log("Path descriptor:", pathDesc)

// Create a command-based descriptor (contains opaque command)
const cmdDesc = cmdDescriptor(Buffer.from("SELECT * FROM users"))
console.log("Command descriptor:", cmdDesc)

// Convert to proto format for wire transmission
const protoDesc = toFlightDescriptor(pathDesc)
const descriptorType = fromDescriptorTypeProto(protoDesc.type)
console.log("Proto descriptor type:", descriptorType?.toUpperCase())
console.log("Proto descriptor path:", protoDesc.path)

// -------------------- Client Configuration --------------------

console.log("\n--- Client Configuration ---\n")

// Create a client with basic authentication
const client = new FlightClient({
  host: "localhost",
  port: 8815,
  tls: false,
  auth: { type: "basic", username: "demo-user", password: "demo-pass" }
})

console.log("Client address:", client.address)
console.log("Client state:", client.state)
console.log("Is connected:", client.isConnected)

// Bearer token authentication
client.setBearerToken("demo-jwt-token")

// Custom headers in metadata
const metadata = client.createMetadata({
  headers: {
    "x-request-id": "req-12345",
    "x-correlation-id": "corr-67890"
  }
})
console.log("Metadata keys:", Object.keys(metadata.toJSON()))

// -------------------- Error Handling --------------------

console.log("\n--- Error Handling ---\n")

// FlightError provides typed error handling with static checkers
const notFoundError = new FlightError("Flight 'unknown' not found", "NOT_FOUND")
console.log("NotFound error:", notFoundError.message)
console.log("Error code:", notFoundError.code)
console.log("Is retriable:", FlightError.isRetriable(notFoundError))
console.log("Is not found:", FlightError.isNotFound(notFoundError))

const unavailableError = new FlightError("Server temporarily down", "UNAVAILABLE")
console.log("\nUnavailable error:", unavailableError.message)
console.log("Is retriable:", FlightError.isRetriable(unavailableError))

// -------------------- TLS Configuration --------------------

console.log("\n--- TLS Configuration (examples) ---\n")

// Basic TLS
console.log("Basic TLS config: { tls: true }")
console.log("Custom CA config: { tls: { rootCerts: Buffer.from(cert) } }")

// mTLS configuration example (commented out - requires certificates):
// import fs from "node:fs"
// const tlsClient = new FlightClient({
//   host: "flight.example.com",
//   port: 443,
//   tls: {
//     rootCerts: fs.readFileSync("/path/to/ca.crt"),
//     certChain: fs.readFileSync("/path/to/client.crt"),
//     privateKey: fs.readFileSync("/path/to/client.key"),
//     serverNameOverride: "flight.example.com"
//   }
// })

// -------------------- RPC Methods Overview --------------------

console.log("\n--- Available RPC Methods ---\n")

const methods = [
  { name: "handshake()", desc: "Authentication handshake" },
  { name: "listFlights()", desc: "List available flights" },
  { name: "getFlightInfo()", desc: "Get metadata about a flight" },
  { name: "getSchema()", desc: "Get Arrow schema for a flight" },
  { name: "doGet()", desc: "Download data stream" },
  { name: "doPut()", desc: "Upload data stream" },
  { name: "doExchange()", desc: "Bidirectional data exchange" },
  { name: "doAction()", desc: "Execute custom action" },
  { name: "listActions()", desc: "List available actions" }
]

for (const m of methods) {
  console.log(`  ${m.name.padEnd(18)} - ${m.desc}`)
}

// -------------------- Usage Example (requires server) --------------------

console.log("\n--- Usage Example (commented out, requires server) ---\n")

console.log(`
// Connect to server
await client.connect()

// List available flights
for await (const info of client.listFlights()) {
  console.log("Flight:", info.flightDescriptor?.path)
}

// Get flight info
const info = await client.getFlightInfo(pathDescriptor("my", "data"))
console.log("Records:", info.totalRecords)

// Download data
for (const endpoint of info.endpoint) {
  for await (const data of client.doGet(endpoint.ticket!)) {
    console.log("Batch size:", data.dataBody.length)
  }
}

// Execute action
for await (const result of client.doAction({ type: "ping", body: Buffer.alloc(0) })) {
  console.log("Action result:", result.body.toString())
}
`)

// -------------------- Cleanup --------------------

client.close()
console.log("Client state after close:", client.state)
console.log("\nDemo complete!")
