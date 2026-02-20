/**
 * Call options example.
 *
 * Demonstrates using call options for timeouts, custom headers,
 * and per-request configuration.
 *
 * @example
 * ```bash
 * bun run examples/call-options.ts
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
    // Example 1: Timeout
    console.log("--- Example 1: Timeout ---")
    await withTimeout(client)

    // Example 2: Custom headers
    console.log("\n--- Example 2: Custom Headers ---")
    await withCustomHeaders(client)

    // Example 3: Request tracing
    console.log("\n--- Example 3: Request Tracing ---")
    await withRequestTracing(client)

    // Example 4: Combined options
    console.log("\n--- Example 4: Combined Options ---")
    await withCombinedOptions(client)
  } finally {
    client.close()
    console.log("\nConnection closed")
  }
}

async function withTimeout(client: Awaited<ReturnType<typeof createFlightClient>>): Promise<void> {
  // Set a timeout for the request in milliseconds
  // If the server doesn't respond within this time, the request fails
  try {
    const info = await client.getFlightInfo(pathDescriptor("my", "dataset"), {
      timeoutMs: 5000 // 5 second timeout
    })

    console.log("  Request completed within timeout")
    console.log("  Total records:", String(info.totalRecords))
  } catch (error) {
    // Timeout errors can be identified by code DEADLINE_EXCEEDED
    console.log("  Request failed (may be timeout or not found)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function withCustomHeaders(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  // Add custom HTTP headers to the request
  // Useful for passing metadata, API keys, or custom context
  try {
    const info = await client.getFlightInfo(pathDescriptor("my", "dataset"), {
      headers: {
        "x-api-key": "your-api-key",
        "x-tenant-id": "tenant-123",
        "x-custom-header": "custom-value"
      }
    })

    console.log("  Request with custom headers completed")
    console.log("  Endpoints:", String(info.endpoint.length))
  } catch (error) {
    console.log("  Request failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function withRequestTracing(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  // Common pattern: add request ID and trace context for observability
  const requestId = crypto.randomUUID()
  const traceId = crypto.randomUUID().replace(/-/g, "")

  try {
    console.log(`  Request ID: ${requestId}`)
    console.log(`  Trace ID: ${traceId}`)

    const info = await client.getFlightInfo(pathDescriptor("my", "dataset"), {
      headers: {
        "x-request-id": requestId,
        "x-trace-id": traceId,
        "x-span-id": traceId.slice(0, 16)
      }
    })

    console.log("  Request traced successfully")
    console.log("  Total bytes:", String(info.totalBytes))
  } catch (error) {
    console.log("  Request failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function withCombinedOptions(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  // Combine multiple options in a single request
  const requestId = crypto.randomUUID()

  try {
    const info = await client.getFlightInfo(pathDescriptor("my", "dataset"), {
      timeoutMs: 10000, // 10 second timeout
      headers: {
        "x-request-id": requestId,
        "x-priority": "high",
        authorization: "Bearer additional-token" // Override auth if needed
      }
    })

    console.log("  Combined options request completed")
    console.log("  Request ID:", requestId)
    console.log("  Schema bytes:", String(info.schema.length))
  } catch (error) {
    console.log("  Request failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
