/**
 * Getting data example.
 *
 * Demonstrates retrieving flight information, schema, and data
 * from a Flight server using getFlightInfo, getSchema, and doGet.
 * Also covers call options (timeouts, headers) and cancellation.
 *
 * @example
 * ```bash
 * bun run examples/getting-data.ts
 * ```
 */
import {
  cmdDescriptor,
  createFlightClient,
  createLocation,
  parseLocation,
  pathDescriptor
} from "../src/index.js"

async function main(): Promise<void> {
  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("Connected to Flight server\n")

  try {
    // Example 1: Get flight info using a path descriptor
    console.log("--- Example 1: Get Flight Info (Path) ---")
    await getFlightInfoByPath(client)

    // Example 2: Get flight info using a command descriptor
    console.log("\n--- Example 2: Get Flight Info (Command) ---")
    await getFlightInfoByCommand(client)

    // Example 3: Get schema only
    console.log("\n--- Example 3: Get Schema ---")
    await getSchemaOnly(client)

    // Example 4: Retrieve data with doGet
    console.log("\n--- Example 4: Retrieve Data (DoGet) ---")
    await retrieveData(client)

    // Example 5: Working with locations
    console.log("\n--- Example 5: Location Utilities ---")
    demonstrateLocationUtilities()

    // Example 6: Call options (timeouts, headers)
    console.log("\n--- Example 6: Call Options ---")
    await withCallOptions(client)

    // Example 7: Stream cancellation
    console.log("\n--- Example 7: Stream Cancellation ---")
    await streamCancellation(client)
  } finally {
    client.close()
    console.log("\nConnection closed")
  }
}

async function getFlightInfoByPath(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    // Create a path-based descriptor
    // Paths identify datasets by hierarchical path segments
    const descriptor = pathDescriptor("database", "schema", "table")

    const info = await client.getFlightInfo(descriptor)

    console.log("  Flight Info:")
    console.log(`    Total records: ${String(info.totalRecords)}`)
    console.log(`    Total bytes: ${String(info.totalBytes)}`)
    console.log(`    Endpoints: ${String(info.endpoint.length)}`)

    // Display endpoint details
    for (let i = 0; i < info.endpoint.length; i++) {
      const endpoint = info.endpoint[i]
      console.log(`\n    Endpoint ${String(i + 1)}:`)

      if (endpoint.ticket !== undefined) {
        console.log(`      Ticket: ${String(endpoint.ticket.ticket.length)} bytes`)
      }

      if (endpoint.location.length > 0) {
        console.log(`      Locations: ${endpoint.location.map((l) => l.uri).join(", ")}`)
      }
    }
  } catch (error) {
    console.log("  Flight not found (expected if path doesn't exist)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function getFlightInfoByCommand(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    // Create a command-based descriptor
    // Commands contain opaque bytes interpreted by the server
    const descriptor = cmdDescriptor(Buffer.from("SELECT * FROM users LIMIT 10"))

    const info = await client.getFlightInfo(descriptor)

    console.log("  Flight Info:")
    console.log(`    Total records: ${String(info.totalRecords)}`)
    console.log(`    Total bytes: ${String(info.totalBytes)}`)
    console.log(`    Endpoints: ${String(info.endpoint.length)}`)
  } catch (error) {
    console.log("  Command not supported (expected for some servers)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function getSchemaOnly(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    const descriptor = pathDescriptor("my", "dataset")

    // getSchema returns only the schema without endpoint information
    // Useful when you only need to inspect the data structure
    const result = await client.getSchema(descriptor)

    console.log("  Schema Result:")
    console.log(`    Schema bytes: ${String(result.schema.length)}`)

    // The schema is IPC-encoded Arrow schema
    // You would typically decode this with Apache Arrow library
    console.log("    (Schema is IPC-encoded, use Arrow library to decode)")
  } catch (error) {
    console.log("  Schema not found (expected if path doesn't exist)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function retrieveData(client: Awaited<ReturnType<typeof createFlightClient>>): Promise<void> {
  try {
    // First, get flight info to obtain tickets
    const descriptor = pathDescriptor("my", "dataset")
    const info = await client.getFlightInfo(descriptor)

    console.log(`  Retrieving data from ${String(info.endpoint.length)} endpoint(s)...`)

    let totalBatches = 0
    let totalBytes = 0

    // Iterate through all endpoints
    for (const endpoint of info.endpoint) {
      if (endpoint.ticket === undefined) {
        console.log("  Skipping endpoint without ticket")
        continue
      }

      // doGet retrieves data for a specific ticket
      // Returns an async iterable of FlightData messages
      for await (const data of client.doGet(endpoint.ticket)) {
        totalBatches++
        totalBytes += data.dataBody.length

        console.log(`    Batch ${String(totalBatches)}:`)
        console.log(`      Header: ${String(data.dataHeader.length)} bytes`)
        console.log(`      Body: ${String(data.dataBody.length)} bytes`)

        if (data.appMetadata.length > 0) {
          console.log(`      App metadata: ${String(data.appMetadata.length)} bytes`)
        }
      }
    }

    console.log(`\n  Total: ${String(totalBatches)} batches, ${String(totalBytes)} bytes`)
  } catch (error) {
    console.log("  Data retrieval failed (expected if path doesn't exist)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

function demonstrateLocationUtilities(): void {
  // parseLocation extracts components from Flight URIs
  // Useful when processing endpoint locations from FlightInfo

  // Parse a secure gRPC location
  const tlsLocation = parseLocation("grpc+tls://flight.example.com:443")
  console.log("  Parsed grpc+tls location:")
  console.log(`    Scheme: ${tlsLocation.scheme}`)
  console.log(`    Host: ${tlsLocation.host}`)
  console.log(`    Port: ${String(tlsLocation.port)}`)
  console.log(`    Secure: ${String(tlsLocation.secure)}`)

  // Parse a plain gRPC location
  const plainLocation = parseLocation("grpc://localhost:8815")
  console.log("\n  Parsed grpc location:")
  console.log(`    Scheme: ${plainLocation.scheme}`)
  console.log(`    Host: ${plainLocation.host}`)
  console.log(`    Port: ${String(plainLocation.port)}`)
  console.log(`    Secure: ${String(plainLocation.secure)}`)

  // Empty string means reuse current connection
  const reuseLocation = parseLocation("")
  console.log("\n  Empty location (reuse connection):")
  console.log(`    Reuse connection: ${String(reuseLocation.reuseConnection)}`)

  // createLocation builds URIs from components
  const created = createLocation("grpc+tls", "data.example.com", 8815)
  console.log("\n  Created location:")
  console.log(`    URI: ${created.uri}`)

  // Unix socket location
  const unixLocation = createLocation("grpc+unix", "/var/run/flight.sock")
  console.log("\n  Unix socket location:")
  console.log(`    URI: ${unixLocation.uri}`)
}

async function withCallOptions(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  // Timeout: Set a deadline for the request
  try {
    const info = await client.getFlightInfo(pathDescriptor("my", "dataset"), {
      timeoutMs: 5000 // 5 second timeout
    })

    console.log("  Request completed within timeout")
    console.log("  Total records:", String(info.totalRecords))
  } catch (error) {
    console.log("  Request failed (may be timeout or not found)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }

  // Custom headers: Add metadata to requests
  try {
    const requestId = crypto.randomUUID()

    const info = await client.getFlightInfo(pathDescriptor("my", "dataset"), {
      timeoutMs: 10000,
      headers: {
        "x-request-id": requestId,
        "x-api-key": "your-api-key",
        "x-tenant-id": "tenant-123"
      }
    })

    console.log("  Request with headers completed")
    console.log(`  Request ID: ${requestId}`)
    console.log(`  Endpoints: ${String(info.endpoint.length)}`)
  } catch (error) {
    console.log("  Request failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function streamCancellation(
  client: Awaited<ReturnType<typeof createFlightClient>>
): Promise<void> {
  try {
    const descriptor = pathDescriptor("large", "dataset")
    const info = await client.getFlightInfo(descriptor)

    if (info.endpoint.length === 0 || info.endpoint[0].ticket === undefined) {
      console.log("  No data available")
      return
    }

    const { ticket } = info.endpoint[0]
    let batchCount = 0
    const maxBatches = 5

    // Process limited batches then cancel
    for await (const data of client.doGet(ticket)) {
      batchCount++
      console.log(`  Batch ${String(batchCount)}: ${String(data.dataBody.length)} bytes`)

      if (batchCount >= maxBatches) {
        console.log(`  Stopping after ${String(maxBatches)} batches (breaking stream)...`)
        break // Breaking exits the async iterator, cleaning up resources
      }
    }

    // For explicit cancellation of FlightInfo:
    // const status = await client.cancelFlightInfo(info)
    // console.log(`  Cancel status: ${status}`)
  } catch (error) {
    console.log("  Operation failed")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
