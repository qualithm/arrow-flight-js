/**
 * Cancellation example.
 *
 * Demonstrates cancelling running flight operations before
 * they complete.
 *
 * @example
 * ```bash
 * bun run examples/cancellation.ts
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
    // Example 1: Cancel a flight by FlightInfo
    console.log("--- Example 1: Cancel Flight ---")
    await cancelFlight(client)

    // Example 2: Timeout-based cancellation
    console.log("\n--- Example 2: Timeout Cancellation ---")
    await timeoutCancellation(client)

    // Example 3: User-initiated cancellation
    console.log("\n--- Example 3: User Cancellation Pattern ---")
    userCancellationPattern(client)
  } finally {
    client.close()
    console.log("\nConnection closed")
  }
}

async function cancelFlight(client: Awaited<ReturnType<typeof createFlightClient>>): Promise<void> {
  // Get flight info for a potentially long-running query
  const descriptor = pathDescriptor("large", "dataset")

  try {
    const info = await client.getFlightInfo(descriptor)

    console.log("  Flight started")
    console.log("  Total records:", info.totalRecords >= 0 ? String(info.totalRecords) : "unknown")

    // Cancel the flight before fetching data
    // This is useful when:
    // - User requests cancellation
    // - Timeout is reached
    // - Application is shutting down
    console.log("\n  Cancelling flight...")
    const status = await client.cancelFlightInfo(info)

    // Handle the cancellation result
    switch (status) {
      case "cancelled": {
        console.log("  Status: cancelled")
        console.log("  Flight cancelled successfully")
        console.log("  Server resources have been released")
        break
      }
      case "cancelling": {
        console.log("  Status: cancelling")
        console.log("  Cancellation in progress")
        console.log("  Server is still processing the cancellation")
        break
      }
      case "not-cancellable": {
        console.log("  Status: not-cancellable")
        console.log("  This flight cannot be cancelled")
        console.log("  Server may not support cancellation or flight already completed")
        break
      }
      case "unspecified": {
        console.log("  Status: unspecified")
        console.log("  Cancellation status unknown")
        break
      }
    }
  } catch (error) {
    console.log("  Operation failed (expected if server doesn't support cancellation)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function timeoutCancellation(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  // Pattern: Start fetching data but cancel if it takes too long
  const descriptor = pathDescriptor("slow", "query")
  const timeoutMs = 5000

  try {
    const info = await client.getFlightInfo(descriptor)

    console.log("  Starting data fetch with timeout...")

    for (const endpoint of info.endpoint) {
      if (endpoint.ticket === undefined) {
        continue
      }

      let receivedBytes = 0
      const startTime = Date.now()

      for await (const data of client.doGet(endpoint.ticket)) {
        receivedBytes += data.dataBody.length

        // Check if timeout exceeded
        const elapsed = Date.now() - startTime
        if (elapsed > timeoutMs) {
          console.log(`  Timeout after ${String(elapsed)}ms`)
          console.log(`  Received ${String(receivedBytes)} bytes before timeout`)

          // Cancel the flight to release server resources
          console.log("  Cancelling flight...")
          const status = await client.cancelFlightInfo(info)
          console.log(`  Cancel status: ${status}`)

          return
        }
      }

      console.log(`  Completed without timeout: ${String(receivedBytes)} bytes`)
    }
  } catch (error) {
    console.log("  Operation failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

function userCancellationPattern(_client: Awaited<ReturnType<typeof createFlightClient>>): void {
  // Pattern: Allow external cancellation signal
  // In practice, this would be triggered by user action (e.g., Ctrl+C, UI button)
  console.log("  Demonstrating cancellation pattern...")
  console.log("")
  console.log("  // Example code for user-cancellable operations:")
  console.log("  let cancelled = false")
  console.log("  let currentFlightInfo: FlightInfo | null = null")
  console.log("")
  console.log("  // Cancel handler (e.g., from UI or signal)")
  console.log("  async function onCancel() {")
  console.log("    cancelled = true")
  console.log("    if (currentFlightInfo) {")
  console.log("      await client.cancelFlightInfo(currentFlightInfo)")
  console.log("    }")
  console.log("  }")
  console.log("")
  console.log("  // Data fetching loop")
  console.log("  async function fetchData() {")
  console.log("    currentFlightInfo = await client.getFlightInfo(descriptor)")
  console.log("    ")
  console.log("    for (const endpoint of currentFlightInfo.endpoint) {")
  console.log("      for await (const data of client.doGet(endpoint.ticket!)) {")
  console.log("        if (cancelled) {")
  console.log('          throw new Error("Operation cancelled by user")')
  console.log("        }")
  console.log("        // Process data...")
  console.log("      }")
  console.log("    }")
  console.log("    ")
  console.log("    currentFlightInfo = null")
  console.log("  }")
}

main().catch(console.error)
