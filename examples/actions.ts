/**
 * Actions example.
 *
 * Demonstrates listing and executing custom server actions
 * using listActions and doAction.
 *
 * @example
 * ```bash
 * bun run examples/actions.ts
 * ```
 */
import { createFlightClient } from "../src/index.js"

async function main(): Promise<void> {
  console.log("=== Actions Examples ===\n")

  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("Connected to Flight server\n")

  try {
    // Example 1: List available actions
    console.log("--- Example 1: List Actions ---")
    await listAvailableActions(client)

    // Example 2: Execute a simple action
    console.log("\n--- Example 2: Simple Action ---")
    await executeSimpleAction(client)

    // Example 3: Action with payload
    console.log("\n--- Example 3: Action with Payload ---")
    await executeActionWithPayload(client)

    // Example 4: Action with multiple results
    console.log("\n--- Example 4: Action with Multiple Results ---")
    await executeActionWithMultipleResults(client)
  } finally {
    client.close()
    console.log("\nConnection closed")
  }
}

async function listAvailableActions(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    console.log("  Available actions:")

    let count = 0
    for await (const actionType of client.listActions()) {
      count++
      console.log(`\n    ${actionType.type}`)

      if (actionType.description.length > 0) {
        console.log(`      ${actionType.description}`)
      }
    }

    if (count === 0) {
      console.log("    No actions available")
    } else {
      console.log(`\n  Total: ${String(count)} action(s)`)
    }
  } catch (error) {
    console.log("  Failed to list actions")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function executeSimpleAction(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    // Execute a no-op action (like a health check)
    const action = {
      type: "healthcheck",
      body: Buffer.alloc(0)
    }

    console.log(`  Executing action: ${action.type}`)

    for await (const result of client.doAction(action)) {
      console.log(`    Result: ${result.body.toString() || "(empty)"}`)
    }

    console.log("  Action completed")
  } catch (error) {
    console.log("  Action failed (action may not be supported)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function executeActionWithPayload(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    // Execute an action that takes a payload
    // The payload format is action-specific
    const payload = JSON.stringify({
      table: "users",
      pattern: "inactive_*"
    })

    const action = {
      type: "drop-tables",
      body: Buffer.from(payload)
    }

    console.log(`  Executing action: ${action.type}`)
    console.log(`    Payload: ${payload}`)

    let resultCount = 0
    for await (const result of client.doAction(action)) {
      resultCount++

      // Try to parse as JSON, otherwise display as string
      const body = result.body.toString()
      try {
        const parsed = JSON.parse(body) as Record<string, unknown>
        console.log(`    Result ${String(resultCount)}:`, parsed)
      } catch {
        console.log(`    Result ${String(resultCount)}: ${body}`)
      }
    }

    console.log(`  Action completed with ${String(resultCount)} result(s)`)
  } catch (error) {
    console.log("  Action failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function executeActionWithMultipleResults(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    // Some actions return multiple results
    // For example, listing cache entries, or processing multiple items
    const action = {
      type: "list-cache",
      body: Buffer.alloc(0)
    }

    console.log(`  Executing action: ${action.type}`)

    const results: string[] = []

    for await (const result of client.doAction(action)) {
      const body = result.body.toString()
      results.push(body)
    }

    console.log(`  Received ${String(results.length)} results:`)
    for (let i = 0; i < results.length; i++) {
      console.log(`    ${String(i + 1)}. ${results[i]}`)
    }
  } catch (error) {
    console.log("  Action failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
