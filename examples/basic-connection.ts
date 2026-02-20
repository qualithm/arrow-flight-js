/**
 * Basic connection example.
 *
 * Demonstrates connecting to a Flight server and listing
 * available flights.
 *
 * @example
 * ```bash
 * bun run examples/basic-connection.ts
 * ```
 */
import { createFlightClient, FlightClient } from "../src/index.js"

async function main(): Promise<void> {
  console.log("=== Basic Connection Examples ===\n")

  // Example 1: One-step connection with createFlightClient
  console.log("--- Example 1: One-Step Connection ---")
  await oneStepConnection()

  // Example 2: Two-step connection with FlightClient
  console.log("\n--- Example 2: Two-Step Connection ---")
  await twoStepConnection()

  // Example 3: List available flights
  console.log("\n--- Example 3: List Flights ---")
  await listFlights()
}

async function oneStepConnection(): Promise<void> {
  // createFlightClient creates and connects in one step
  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("  Connected:", client.isConnected)
  console.log("  Address:", client.address)
  console.log("  State:", client.state)

  client.close()
  console.log("  Connection closed")
}

async function twoStepConnection(): Promise<void> {
  // Create client instance first
  const client = new FlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("  State before connect:", client.state)
  console.log("  isConnected before:", client.isConnected)

  // Connect explicitly
  await client.connect()

  console.log("  State after connect:", client.state)
  console.log("  isConnected after:", client.isConnected)

  // Close when done
  client.close()
  console.log("  State after close:", client.state)
}

async function listFlights(): Promise<void> {
  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("  Listing available flights:")

  try {
    let count = 0

    // listFlights returns an async iterable
    for await (const info of client.listFlights()) {
      count++
      console.log(`\n  Flight ${String(count)}:`)

      // Display descriptor information
      if (info.flightDescriptor !== undefined) {
        const desc = info.flightDescriptor
        if (desc.path.length > 0) {
          console.log(`    Path: ${desc.path.join("/")}`)
        }
        if (desc.cmd.length > 0) {
          console.log(`    Command: ${desc.cmd.toString()}`)
        }
      }

      // Display size information
      if (info.totalRecords >= 0) {
        console.log(`    Records: ${String(info.totalRecords)}`)
      }
      if (info.totalBytes >= 0) {
        console.log(`    Bytes: ${String(info.totalBytes)}`)
      }

      // Display endpoint count
      console.log(`    Endpoints: ${String(info.endpoint.length)}`)
    }

    if (count === 0) {
      console.log("    No flights available")
    } else {
      console.log(`\n  Total: ${String(count)} flight(s)`)
    }
  } finally {
    client.close()
    console.log("\n  Connection closed")
  }
}

main().catch(console.error)
