/**
 * Error handling example.
 *
 * Demonstrates handling various error types from Flight operations.
 *
 * @example
 * ```bash
 * bun run examples/error-handling.ts
 * ```
 */
import { createFlightClient, FlightError, pathDescriptor } from "../src/index.js"

async function main(): Promise<void> {
  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("Connected to Flight server\n")

  try {
    // Example 1: Not found error
    console.log("--- Example 1: Not Found ---")
    await handleNotFound(client)

    // Example 2: Authentication errors
    console.log("\n--- Example 2: Authentication Errors ---")
    await handleAuthErrors()

    // Example 3: Retriable errors
    console.log("\n--- Example 3: Retriable Errors ---")
    await handleRetriableErrors(client)

    // Example 4: Error inspection
    console.log("\n--- Example 4: Error Inspection ---")
    await inspectErrors(client)

    // Example 5: Error classification
    console.log("\n--- Example 5: Error Classification ---")
    showErrorClassification()
  } finally {
    client.close()
    console.log("\nConnection closed")
  }
}

async function handleNotFound(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    await client.getFlightInfo(pathDescriptor("nonexistent", "path"))
  } catch (error) {
    if (FlightError.isNotFound(error)) {
      console.log("  Caught NOT_FOUND error")
      console.log("  The requested flight does not exist")
      console.log("  Action: Check the path or list available flights")
    } else {
      console.log("  Unexpected error:", error)
    }
  }
}

async function handleAuthErrors(): Promise<void> {
  try {
    // Try connecting with invalid credentials
    const client = await createFlightClient({
      host: "localhost",
      port: 8815,
      tls: false,
      auth: {
        type: "bearer",
        token: "invalid-token"
      }
    })

    // This may fail on any operation if auth is required
    for await (const _ of client.listFlights()) {
      // ...
    }
  } catch (error) {
    if (FlightError.isUnauthenticated(error)) {
      console.log("  Caught UNAUTHENTICATED error")
      console.log("  Invalid or missing credentials")
      console.log("  Action: Check token or credentials")
    } else if (FlightError.isPermissionDenied(error)) {
      console.log("  Caught PERMISSION_DENIED error")
      console.log("  Valid credentials but insufficient permissions")
      console.log("  Action: Request access or use different credentials")
    } else if (error instanceof Error) {
      console.log("  Connection or auth error:", error.message)
    }
  }
}

async function handleRetriableErrors(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  const maxRetries = 3
  const baseDelayMs = 1000

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      // Attempt the operation
      await client.getFlightInfo(pathDescriptor("flaky", "service"))
      console.log("  Operation succeeded")
      return
    } catch (error) {
      if (FlightError.isRetriable(error) && attempt < maxRetries) {
        // Exponential backoff for retriable errors
        const delay = baseDelayMs * Math.pow(2, attempt - 1)
        console.log(`  Attempt ${String(attempt)} failed with retriable error`)
        console.log(`  Retrying in ${String(delay)}ms...`)
        await new Promise((r) => setTimeout(r, delay))
      } else if (FlightError.isRetriable(error)) {
        console.log(`  All ${String(maxRetries)} attempts failed`)
        console.log("  Error:", error instanceof Error ? error.message : error)
      } else {
        // Non-retriable error, fail immediately
        console.log("  Non-retriable error:", error instanceof Error ? error.message : error)
        return
      }
    }
  }
}

async function inspectErrors(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    await client.getFlightInfo(pathDescriptor("inspect", "error"))
  } catch (error) {
    if (error instanceof FlightError) {
      console.log("  FlightError details:")
      console.log("    Message:", error.message)
      console.log("    Code:", error.code)
      console.log("    gRPC code:", error.grpcCode ?? "N/A")

      // Additional details if available
      if (error.details !== undefined) {
        console.log("    Details:", error.details)
      }

      // Metadata attached to the error
      if (error.metadata !== undefined) {
        console.log("    Metadata keys:", Object.keys(error.metadata).join(", ") || "none")
      }

      // Stack trace for debugging
      // console.log("    Stack:", error.stack)
    } else {
      console.log("  Non-Flight error:", error)
    }
  }
}

function showErrorClassification(): void {
  console.log("  FlightError helper methods:")
  console.log("")
  console.log("  // Check specific error types")
  console.log("  FlightError.isNotFound(error)        // Resource doesn't exist")
  console.log("  FlightError.isUnauthenticated(error) // Missing or invalid credentials")
  console.log("  FlightError.isPermissionDenied(error)// Insufficient permissions")
  console.log("  FlightError.isInvalidArgument(error) // Bad request parameters")
  console.log("  FlightError.isAlreadyExists(error)   // Duplicate resource")
  console.log("  FlightError.isResourceExhausted(error) // Quota or rate limit")
  console.log("  FlightError.isCancelled(error)       // Operation was cancelled")
  console.log("  FlightError.isDeadlineExceeded(error)// Timeout")
  console.log("  FlightError.isUnavailable(error)     // Service unavailable")
  console.log("")
  console.log("  // Check if error is worth retrying")
  console.log("  FlightError.isRetriable(error)       // UNAVAILABLE, RESOURCE_EXHAUSTED")
  console.log("")
  console.log("  // Type guard")
  console.log("  FlightError.isFlightError(error)     // Check if FlightError instance")
  console.log("")
  console.log("  // Error codes (FlightErrorCode type):")
  console.log("  // OK, CANCELLED, UNKNOWN, INVALID_ARGUMENT, DEADLINE_EXCEEDED,")
  console.log("  // NOT_FOUND, ALREADY_EXISTS, PERMISSION_DENIED, RESOURCE_EXHAUSTED,")
  console.log("  // FAILED_PRECONDITION, ABORTED, OUT_OF_RANGE, UNIMPLEMENTED,")
  console.log("  // INTERNAL, UNAVAILABLE, DATA_LOSS, UNAUTHENTICATED")
}

main().catch(console.error)
