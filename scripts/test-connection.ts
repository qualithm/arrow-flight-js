#!/usr/bin/env bun
/**
 * Demo script to test Arrow Flight server connectivity.
 *
 * Usage:
 *   bun run demo
 *
 * Environment variables:
 *   FLIGHT_HOST - Server hostname (default: localhost)
 *   FLIGHT_PORT - Server port (default: 8815)
 *   FLIGHT_TLS  - Enable TLS (default: false)
 */

import { FlightClient } from "../src/index.js"

const host = process.env.FLIGHT_HOST ?? "localhost"
const port = Number(process.env.FLIGHT_PORT ?? 8815)
const tls = process.env.FLIGHT_TLS === "true"

console.log("Arrow Flight Connection Test")
console.log("============================")
console.log(`Host: ${host}:${String(port)}`)
console.log(`TLS:  ${String(tls)}\n`)

try {
  const client = new FlightClient({ host, port, tls })
  await client.connect()
  console.log("Connected successfully!")
  client.close()
} catch (error) {
  console.error("Connection failed:", error instanceof Error ? error.message : String(error))
  process.exit(1)
}
