/**
 * Streaming example.
 *
 * Demonstrates processing large result sets without loading
 * everything into memory at once.
 *
 * @example
 * ```bash
 * bun run examples/streaming.ts
 * ```
 */
import { createFlightClient, pathDescriptor } from "../src/index.js"

async function main(): Promise<void> {
  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("Connected to Flight server\n")

  try {
    // Example 1: Stream data from a single endpoint
    console.log("--- Example 1: Single Endpoint Streaming ---")
    await streamSingleEndpoint(client)

    // Example 2: Stream from multiple endpoints
    console.log("\n--- Example 2: Multi-Endpoint Streaming ---")
    await streamMultipleEndpoints(client)

    // Example 3: Process with batch counting
    console.log("\n--- Example 3: Batch Processing ---")
    await processBatches(client)

    // Example 4: Memory-efficient processing
    console.log("\n--- Example 4: Memory-Efficient Processing ---")
    await memoryEfficientProcessing(client)
  } finally {
    client.close()
    console.log("\nConnection closed")
  }
}

async function streamSingleEndpoint(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    const descriptor = pathDescriptor("large", "dataset")
    const info = await client.getFlightInfo(descriptor)

    console.log("  Flight Info:")
    console.log(`    Total records: ${String(info.totalRecords)}`)
    console.log(`    Total bytes: ${String(info.totalBytes)}`)
    console.log(`    Endpoints: ${String(info.endpoint.length)}`)

    if (info.endpoint.length === 0 || info.endpoint[0].ticket === undefined) {
      console.log("  No data available")
      return
    }

    // Stream from the first endpoint
    const { ticket } = info.endpoint[0]

    console.log("\n  Streaming data:")

    let batchCount = 0
    let totalBytes = 0

    // doGet returns an async iterable - data is processed as it arrives
    for await (const data of client.doGet(ticket)) {
      batchCount++
      totalBytes += data.dataBody.length

      // Process each batch without holding all data in memory
      console.log(`    Batch ${String(batchCount)}: ${String(data.dataBody.length)} bytes`)

      // Yield control to prevent blocking (for UI responsiveness, etc.)
      if (batchCount % 10 === 0) {
        await new Promise((resolve) => setImmediate(resolve))
      }
    }

    console.log(`\n  Total: ${String(batchCount)} batches, ${String(totalBytes)} bytes`)
  } catch (error) {
    console.log("  Streaming failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function streamMultipleEndpoints(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    const descriptor = pathDescriptor("partitioned", "dataset")
    const info = await client.getFlightInfo(descriptor)

    console.log(`  Dataset has ${String(info.endpoint.length)} partition(s)`)

    let totalBatches = 0
    let totalBytes = 0

    // Process each endpoint (partition) sequentially
    for (let i = 0; i < info.endpoint.length; i++) {
      const endpoint = info.endpoint[i]

      if (endpoint.ticket === undefined) {
        console.log(`  Partition ${String(i + 1)}: No ticket`)
        continue
      }

      console.log(`\n  Partition ${String(i + 1)}:`)

      let partitionBatches = 0
      let partitionBytes = 0

      for await (const data of client.doGet(endpoint.ticket)) {
        partitionBatches++
        partitionBytes += data.dataBody.length
      }

      console.log(`    ${String(partitionBatches)} batches, ${String(partitionBytes)} bytes`)

      totalBatches += partitionBatches
      totalBytes += partitionBytes
    }

    console.log(`\n  Grand total: ${String(totalBatches)} batches, ${String(totalBytes)} bytes`)
  } catch (error) {
    console.log("  Multi-endpoint streaming failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function processBatches(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    const descriptor = pathDescriptor("analytics", "events")
    const info = await client.getFlightInfo(descriptor)

    if (info.endpoint.length === 0 || info.endpoint[0].ticket === undefined) {
      console.log("  No data available")
      return
    }

    const { ticket } = info.endpoint[0]

    // Track statistics while streaming
    const stats = {
      batches: 0,
      bytes: 0,
      minBatchSize: Infinity,
      maxBatchSize: 0
    }

    const startTime = Date.now()

    for await (const data of client.doGet(ticket)) {
      stats.batches++
      stats.bytes += data.dataBody.length
      stats.minBatchSize = Math.min(stats.minBatchSize, data.dataBody.length)
      stats.maxBatchSize = Math.max(stats.maxBatchSize, data.dataBody.length)
    }

    const elapsed = Date.now() - startTime

    console.log("  Processing Statistics:")
    console.log(`    Total batches: ${String(stats.batches)}`)
    console.log(`    Total bytes: ${String(stats.bytes)}`)
    console.log(`    Min batch size: ${String(stats.minBatchSize)}`)
    console.log(`    Max batch size: ${String(stats.maxBatchSize)}`)
    console.log(`    Avg batch size: ${String(Math.round(stats.bytes / stats.batches))}`)
    console.log(`    Elapsed time: ${String(elapsed)}ms`)
    console.log(`    Throughput: ${String(Math.round(stats.bytes / (elapsed / 1000) / 1024))} KB/s`)
  } catch (error) {
    console.log("  Batch processing failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function memoryEfficientProcessing(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    const descriptor = pathDescriptor("huge", "dataset")
    const info = await client.getFlightInfo(descriptor)

    // Check estimated size before fetching
    const estimatedBytes = info.totalBytes
    console.log(`  Estimated size: ${String(estimatedBytes)} bytes`)

    if (estimatedBytes > 1024 * 1024 * 1024) {
      console.log("  Warning: Dataset is > 1GB, processing in chunks")
    }

    // Process without keeping data in memory
    // This simulates writing to disk or forwarding to another system
    let processedRecords = BigInt(0)
    let processedBytes = BigInt(0)

    for (const endpoint of info.endpoint) {
      if (endpoint.ticket === undefined) {
        continue
      }

      for await (const data of client.doGet(endpoint.ticket)) {
        // In a real app, you might:
        // - Write to disk
        // - Forward to another service
        // - Transform and aggregate
        // - Insert into a database

        processedBytes += BigInt(data.dataBody.length)

        // Simulate processing without keeping data
        // The data variable goes out of scope after each iteration
      }
    }

    const { totalRecords } = info
    if (totalRecords >= 0) {
      processedRecords = BigInt(totalRecords)
    }

    console.log("  Memory-efficient processing complete:")
    console.log(`    Records processed: ${String(processedRecords)}`)
    console.log(`    Bytes processed: ${String(processedBytes)}`)
    console.log("    Memory footprint: minimal (streamed)")
  } catch (error) {
    console.log("  Memory-efficient processing failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
