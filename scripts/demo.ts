#!/usr/bin/env bun
/**
 * Demo script showcasing arrow-flight-js.
 *
 * Run with: bun run demo
 */

import {
  cmdDescriptor,
  FlightClient,
  fromDescriptorTypeProto,
  pathDescriptor,
  toFlightDescriptor
} from "../src/index.js"

console.log("Arrow Flight JS Demo")
console.log("====================\n")

// Create a path-based descriptor
const pathDesc = pathDescriptor("database", "schema", "table")
console.log("Path descriptor:", pathDesc)

// Create a command-based descriptor
const cmdDesc = cmdDescriptor(Buffer.from("SELECT * FROM users"))
console.log("Command descriptor:", cmdDesc)

// Convert to proto format
const protoDesc = toFlightDescriptor(pathDesc)
const descriptorType = fromDescriptorTypeProto(protoDesc.type)
console.log("Proto descriptor type:", descriptorType?.toUpperCase())
console.log("Proto descriptor path:", protoDesc.path)

console.log("\n--- FlightClient ---\n")

// Create a client (without connecting)
const client = new FlightClient({
  host: "localhost",
  port: 8815,
  tls: false,
  auth: { type: "basic", username: "demo-user", password: "demo-pass" }
})

console.log("Client address:", client.address)
console.log("Client state:", client.state)
console.log("Is connected:", client.isConnected)

// Demonstrate metadata creation
client.setBearerToken("demo-token")
const metadata = client.createMetadata({ headers: { "x-request-id": "demo-123" } })
console.log("Metadata keys:", Object.keys(metadata.toJSON()))

// TLS configuration examples (commented out):
// const tlsClient = new FlightClient({
//   host: "flight.example.com",
//   tls: {
//     rootCerts: fs.readFileSync('/path/to/ca.crt'),
//     certChain: fs.readFileSync('/path/to/client.crt'),
//     privateKey: fs.readFileSync('/path/to/client.key'),
//     serverNameOverride: "flight.example.com",
//     verifyServerCert: true
//   }
// })

// Handshake example (commented out - requires server):
// await client.connect()
// const result = await client.handshake()
// console.log("Handshake token:", result.token)

client.close()
console.log("Client state after close:", client.state)
