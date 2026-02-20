/**
 * Data exchange example.
 *
 * Demonstrates bidirectional data exchange with a Flight server
 * using doExchange.
 *
 * @example
 * ```bash
 * bun run examples/exchange.ts
 * ```
 */
import { cmdDescriptor, createFlightClient } from "../src/index.js"

async function main(): Promise<void> {
  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("Connected to Flight server\n")

  try {
    // Example 1: Simple exchange
    console.log("--- Example 1: Simple Exchange ---")
    await simpleExchange(client)

    // Example 2: Transform exchange
    console.log("\n--- Example 2: Transform Exchange ---")
    await transformExchange(client)

    // Example 3: Interactive exchange
    console.log("\n--- Example 3: Interactive Exchange ---")
    await interactiveExchange(client)
  } finally {
    client.close()
    console.log("\nConnection closed")
  }
}

async function simpleExchange(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    // Create a DoExchange stream
    // This is a bidirectional stream - you can send and receive simultaneously
    const stream = client.doExchange()

    // Send a command descriptor to indicate what operation to perform
    const descriptor = cmdDescriptor(Buffer.from("echo"))

    stream.write({
      flightDescriptor: {
        type: 2, // CMD type
        path: [],
        cmd: descriptor.cmd
      },
      dataHeader: Buffer.alloc(0),
      appMetadata: Buffer.from("hello-exchange"),
      dataBody: Buffer.from("test-data")
    })

    console.log("  Sent exchange request")

    // End the write side
    stream.end()

    // Collect results
    let resultCount = 0
    for await (const data of stream.results()) {
      resultCount++
      console.log(`  Received response ${String(resultCount)}:`)
      console.log(`    Header: ${String(data.dataHeader.length)} bytes`)
      console.log(`    Body: ${String(data.dataBody.length)} bytes`)

      if (data.appMetadata.length > 0) {
        console.log(`    Metadata: ${data.appMetadata.toString()}`)
      }
    }

    console.log(`  Exchange complete, ${String(resultCount)} responses`)
  } catch (error) {
    console.log("  Exchange failed (server may not support doExchange)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function transformExchange(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    // DoExchange can be used for streaming transformations
    // Send data to be transformed and receive results back
    const stream = client.doExchange()

    // Send transform command
    stream.write({
      flightDescriptor: {
        type: 2,
        path: [],
        cmd: Buffer.from("transform:uppercase")
      },
      dataHeader: Buffer.alloc(0),
      appMetadata: Buffer.alloc(0),
      dataBody: Buffer.alloc(0)
    })

    console.log("  Sent transform command")

    // Send data to transform
    const inputs = ["hello", "world", "flight"]

    for (const input of inputs) {
      stream.write({
        flightDescriptor: undefined,
        dataHeader: Buffer.alloc(0),
        appMetadata: Buffer.alloc(0),
        dataBody: Buffer.from(input)
      })
      console.log(`    Sent: ${input}`)
    }

    stream.end()

    // Receive transformed results
    console.log("  Receiving transformed results:")
    for await (const data of stream.results()) {
      console.log(`    Received: ${data.dataBody.toString()}`)
    }
  } catch (error) {
    console.log("  Transform exchange failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function interactiveExchange(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    // Demonstrates interleaved send/receive
    // Useful for interactive protocols or request-response patterns
    const stream = client.doExchange()

    // Send initial request
    stream.write({
      flightDescriptor: {
        type: 2,
        path: [],
        cmd: Buffer.from("interactive")
      },
      dataHeader: Buffer.alloc(0),
      appMetadata: Buffer.from("request-1"),
      dataBody: Buffer.alloc(0)
    })

    console.log("  Sent request 1")

    // In a real scenario, you might process responses as they arrive
    // while continuing to send data

    // Set up result processing
    const resultsPromise = (async () => {
      const responses: string[] = []
      for await (const data of stream.results()) {
        responses.push(data.appMetadata.toString() || `body:${String(data.dataBody.length)}`)
      }
      return responses
    })()

    // Send more requests
    stream.write({
      flightDescriptor: undefined,
      dataHeader: Buffer.alloc(0),
      appMetadata: Buffer.from("request-2"),
      dataBody: Buffer.alloc(0)
    })

    console.log("  Sent request 2")

    stream.write({
      flightDescriptor: undefined,
      dataHeader: Buffer.alloc(0),
      appMetadata: Buffer.from("request-3"),
      dataBody: Buffer.alloc(0)
    })

    console.log("  Sent request 3")

    // Signal we're done sending
    stream.end()

    // Wait for all responses
    const responses = await resultsPromise
    console.log(`  Received ${String(responses.length)} responses:`)
    for (const response of responses) {
      console.log(`    - ${response}`)
    }
  } catch (error) {
    console.log("  Interactive exchange failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
