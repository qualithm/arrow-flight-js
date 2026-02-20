/**
 * Putting data example.
 *
 * Demonstrates uploading data to a Flight server using doPut.
 *
 * @example
 * ```bash
 * bun run examples/putting-data.ts
 * ```
 */
import { createFlightClient, pathDescriptor } from "../src/index.js"

async function main(): Promise<void> {
  console.log("=== Putting Data Examples ===\n")

  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("Connected to Flight server\n")

  try {
    // Example 1: Simple data upload
    console.log("--- Example 1: Simple Data Upload ---")
    await simpleUpload(client)

    // Example 2: Upload with streaming
    console.log("\n--- Example 2: Streaming Upload ---")
    await streamingUpload(client)

    // Example 3: Upload with app metadata
    console.log("\n--- Example 3: Upload with Metadata ---")
    await uploadWithMetadata(client)
  } finally {
    client.close()
    console.log("\nConnection closed")
  }
}

async function simpleUpload(client: Awaited<ReturnType<typeof createFlightClient>>): Promise<void> {
  try {
    // Create a DoPut stream
    const stream = client.doPut()

    // Convert path descriptor for the message
    const descriptor = pathDescriptor("uploads", "my-table")

    // Create mock Arrow IPC data
    // In a real application, you would use the Apache Arrow library
    // to serialize your data to IPC format
    const mockSchemaHeader = Buffer.from([0, 0, 0, 0]) // Placeholder
    const mockDataBody = Buffer.from("mock-arrow-data")

    // Send the first message with the flight descriptor
    // This tells the server what dataset we're uploading to
    stream.write({
      flightDescriptor: {
        type: 1, // PATH type
        path: descriptor.path,
        cmd: Buffer.alloc(0)
      },
      dataHeader: mockSchemaHeader,
      appMetadata: Buffer.alloc(0),
      dataBody: Buffer.alloc(0)
    })

    console.log("  Sent descriptor and schema")

    // Send a data batch
    stream.write({
      flightDescriptor: undefined,
      dataHeader: Buffer.from([1, 0, 0, 0]), // Record batch indicator
      appMetadata: Buffer.alloc(0),
      dataBody: mockDataBody
    })

    console.log("  Sent data batch")

    // End the stream
    stream.end()

    // Collect acknowledgements
    const results = await stream.collectResults()

    console.log(`  Received ${String(results.length)} acknowledgement(s)`)
    for (let i = 0; i < results.length; i++) {
      const result = results[i]
      console.log(`    Ack ${String(i + 1)}: ${String(result.appMetadata.length)} bytes metadata`)
    }
  } catch (error) {
    console.log("  Upload failed (server may not support doPut)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function streamingUpload(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    const stream = client.doPut()

    const descriptor = pathDescriptor("uploads", "streaming-table")

    // Send descriptor first
    stream.write({
      flightDescriptor: {
        type: 1,
        path: descriptor.path,
        cmd: Buffer.alloc(0)
      },
      dataHeader: Buffer.from([0, 0, 0, 0]),
      appMetadata: Buffer.alloc(0),
      dataBody: Buffer.alloc(0)
    })

    // Simulate streaming multiple batches
    const batchCount = 5
    console.log(`  Streaming ${String(batchCount)} batches...`)

    for (let i = 0; i < batchCount; i++) {
      // Create batch data (in a real app, this would be Arrow IPC data)
      const batchData = Buffer.from(`batch-${String(i)}-data`)

      stream.write({
        flightDescriptor: undefined,
        dataHeader: Buffer.from([1, 0, 0, 0]),
        appMetadata: Buffer.alloc(0),
        dataBody: batchData
      })

      console.log(`    Sent batch ${String(i + 1)}`)
    }

    stream.end()

    // Process acknowledgements as they arrive
    console.log("  Waiting for acknowledgements...")

    // Use the async iterator to get acks as they arrive
    for await (const result of stream.results()) {
      console.log(`    Received ack: ${String(result.appMetadata.length)} bytes`)
    }

    console.log("  Upload complete")
  } catch (error) {
    console.log("  Streaming upload failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function uploadWithMetadata(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    const stream = client.doPut()

    const descriptor = pathDescriptor("uploads", "metadata-table")

    // Include application metadata with each message
    // This can be used for custom metadata like compression info,
    // checksums, sequence numbers, etc.
    const appMetadata = Buffer.from(
      JSON.stringify({
        timestamp: new Date().toISOString(),
        format: "arrow-ipc",
        compression: "none"
      })
    )

    stream.write({
      flightDescriptor: {
        type: 1,
        path: descriptor.path,
        cmd: Buffer.alloc(0)
      },
      dataHeader: Buffer.from([0, 0, 0, 0]),
      appMetadata,
      dataBody: Buffer.alloc(0)
    })

    console.log("  Sent descriptor with metadata")

    // Send data with per-batch metadata
    for (let i = 0; i < 3; i++) {
      const batchMetadata = Buffer.from(
        JSON.stringify({
          batchIndex: i,
          rowCount: 1000
        })
      )

      stream.write({
        flightDescriptor: undefined,
        dataHeader: Buffer.from([1, 0, 0, 0]),
        appMetadata: batchMetadata,
        dataBody: Buffer.from(`batch-${String(i)}-payload`)
      })

      console.log(`    Sent batch ${String(i + 1)} with metadata`)
    }

    stream.end()
    const results = await stream.collectResults()

    console.log(`  Upload complete, ${String(results.length)} acks received`)
  } catch (error) {
    console.log("  Upload with metadata failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
