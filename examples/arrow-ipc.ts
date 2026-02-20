/**
 * Arrow IPC example.
 *
 * Demonstrates using the Apache Arrow library to serialise and
 * deserialise data in Arrow IPC format for Flight operations.
 *
 * @example
 * ```bash
 * bun run examples/arrow-ipc.ts
 * ```
 */
import { tableFromArrays, tableFromIPC, tableToIPC } from "apache-arrow"

import { createFlightClient, pathDescriptor } from "../src/index.js"

async function main(): Promise<void> {
  console.log("=== Arrow IPC Examples ===\n")

  // Example 1: Create Arrow Table
  console.log("--- Example 1: Create Arrow Table ---")
  demonstrateTableCreation()

  // Example 2: Serialise to IPC format
  console.log("\n--- Example 2: Serialise to IPC ---")
  demonstrateIpcSerialisation()

  // Example 3: Parse IPC data from Flight
  console.log("\n--- Example 3: Parse IPC Data ---")
  demonstrateIpcParsing()

  // Example 4: Full round-trip with doPut
  console.log("\n--- Example 4: Full DoPut Round-Trip ---")
  await demonstrateDoPutWithArrow()

  // Example 5: Full round-trip with doGet
  console.log("\n--- Example 5: Full DoGet Round-Trip ---")
  await demonstrateDoGetWithArrow()
}

function demonstrateTableCreation(): void {
  // Create a Table from plain JavaScript arrays
  // tableFromArrays automatically infers types from the data
  const table = tableFromArrays({
    id: Int32Array.from([1, 2, 3, 4, 5]),
    name: ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    score: Float64Array.from([95.5, 87.0, 92.5, 88.0, 91.5])
  })

  console.log("  Created Table from arrays:")
  console.log(`    Rows: ${String(table.numRows)}`)
  console.log(`    Columns: ${String(table.numCols)}`)

  // Display schema
  const fieldNames = table.schema.fields.map((f) => `${f.name}: ${String(f.type)}`)
  console.log(`    Schema: ${fieldNames.join(", ")}`)

  // Access data
  console.log(`\n  Sample data:`)
  for (let i = 0; i < Math.min(3, table.numRows); i++) {
    const row = table.get(i)
    if (row !== null) {
      console.log(`    Row ${String(i)}: ${JSON.stringify(row.toJSON())}`)
    }
  }
}

function demonstrateIpcSerialisation(): void {
  // Create sample data using native typed arrays
  const table = tableFromArrays({
    id: Int32Array.from([100, 200, 300]),
    value: ["first", "second", "third"]
  })

  // Serialise to IPC stream format
  // "stream" format is used by Flight for streaming data
  const ipcBytes = tableToIPC(table, "stream")

  console.log("  IPC Serialisation:")
  console.log(`    Input rows: ${String(table.numRows)}`)
  console.log(`    Output bytes: ${String(ipcBytes.byteLength)}`)
  console.log(`    Format: Arrow IPC Stream`)

  // The IPC format includes:
  // 1. Schema message (field names, types, metadata)
  // 2. RecordBatch messages (columnar data)
  // 3. End-of-stream marker

  // Parse it back to verify round-trip
  const parsedTable = tableFromIPC(ipcBytes)
  const columnNames = parsedTable.schema.fields.map((f) => f.name)
  console.log(`\n  Parsed back:`)
  console.log(`    Rows: ${String(parsedTable.numRows)}`)
  console.log(`    Columns: ${columnNames.join(", ")}`)
}

function demonstrateIpcParsing(): void {
  // Simulate receiving Flight data
  // In a real scenario, this comes from doGet()

  // Create sample IPC data (as if received from server)
  const table = tableFromArrays({
    student_id: Int32Array.from([1, 2, 3]),
    score: Int32Array.from([95, 87, 92])
  })
  const ipcBytes = tableToIPC(table, "stream")

  console.log("  Simulated Flight response:")
  console.log(`    Received ${String(ipcBytes.byteLength)} bytes`)

  // Parse the IPC data
  const parsedTable = tableFromIPC(ipcBytes)

  const schemaDesc = parsedTable.schema.fields.map((f) => `${f.name}: ${String(f.type)}`)
  console.log(`\n  Parsed table:`)
  console.log(`    Schema: ${schemaDesc.join(", ")}`)
  console.log(`    Rows: ${String(parsedTable.numRows)}`)

  // Convert to JavaScript objects
  console.log(`\n  As JavaScript objects:`)
  for (let i = 0; i < Math.min(3, parsedTable.numRows); i++) {
    const row = parsedTable.get(i)
    if (row !== null) {
      console.log(`    ${JSON.stringify(row.toJSON())}`)
    }
  }
}

async function demonstrateDoPutWithArrow(): Promise<void> {
  try {
    const client = await createFlightClient({
      host: "localhost",
      port: 8815,
      tls: false
    })

    // Create Arrow data to upload using typed arrays
    const table = tableFromArrays({
      timestamp: Int32Array.from([1000, 2000, 3000]),
      event_type: ["click", "view", "purchase"]
    })

    // Serialise to IPC format
    const ipcBytes = tableToIPC(table, "stream")
    const ipcBuffer = Buffer.from(ipcBytes)

    console.log("  Prepared Arrow data:")
    console.log(`    Rows: ${String(table.numRows)}`)
    console.log(`    IPC bytes: ${String(ipcBuffer.length)}`)

    // Create the doPut stream
    const stream = client.doPut()

    // Send descriptor with schema
    // The first message includes the flight descriptor
    const descriptor = pathDescriptor("events", "user_actions")

    stream.write({
      flightDescriptor: {
        type: 1, // PATH type
        path: descriptor.path,
        cmd: Buffer.alloc(0)
      },
      // In a full implementation, split IPC into header/body
      // For simplicity, we send the entire IPC as dataBody
      dataHeader: Buffer.alloc(0),
      appMetadata: Buffer.alloc(0),
      dataBody: ipcBuffer
    })

    console.log("  Sent Arrow data to server")

    // End the stream
    stream.end()

    // Wait for acknowledgements
    const results = await stream.collectResults()
    console.log(`  Received ${String(results.length)} acknowledgement(s)`)

    client.close()
    console.log("  Connection closed")
  } catch (error) {
    console.log("  Upload failed (expected if no server running)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function demonstrateDoGetWithArrow(): Promise<void> {
  try {
    const client = await createFlightClient({
      host: "localhost",
      port: 8815,
      tls: false
    })

    console.log("  Requesting flight data...")

    // Get flight info
    const descriptor = pathDescriptor("sample", "data")
    const info = await client.getFlightInfo(descriptor)

    console.log(`  Flight has ${String(info.endpoint.length)} endpoint(s)`)

    // Collect IPC data from all endpoints
    const ipcChunks: Buffer[] = []

    for (const endpoint of info.endpoint) {
      if (endpoint.ticket === undefined) {
        continue
      }

      for await (const flightData of client.doGet(endpoint.ticket)) {
        // Collect the IPC data
        // In Flight, dataHeader contains the IPC message header
        // and dataBody contains the message body
        if (flightData.dataBody.length > 0) {
          ipcChunks.push(flightData.dataBody)
        }
      }
    }

    console.log(`  Received ${String(ipcChunks.length)} data chunk(s)`)

    // Parse the combined IPC data
    if (ipcChunks.length > 0) {
      const combined = Buffer.concat(ipcChunks)

      // Note: Parsing depends on how the server sends data
      // This is a simplified example - real parsing may need
      // to handle message framing
      try {
        const table = tableFromIPC(combined)
        const columnNames = table.schema.fields.map((f) => f.name)
        console.log(`\n  Parsed Arrow table:`)
        console.log(`    Schema: ${columnNames.join(", ")}`)
        console.log(`    Rows: ${String(table.numRows)}`)
      } catch {
        console.log("  (IPC data requires additional framing to parse)")
      }
    }

    client.close()
    console.log("  Connection closed")
  } catch (error) {
    console.log("  Fetch failed (expected if no server running)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
