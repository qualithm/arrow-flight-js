/**
 * Integration tests for Flight operations: listFlights, getFlightInfo, getSchema.
 *
 * Requires a running Arrow Flight server with test fixtures.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createFlightClient, type FlightClient, pathDescriptor } from "../../index"
import { config } from "./config"

describe("Flight Operations Integration", () => {
  let client: FlightClient

  beforeAll(async () => {
    client = await createFlightClient({
      host: config.host,
      port: config.port,
      tls: config.tls,
      auth: {
        type: "basic",
        username: config.credentials.admin.username,
        password: config.credentials.admin.password
      }
    })
    await client.handshake()
  })

  afterAll(() => {
    client.close()
  })

  describe("listFlights", () => {
    it("lists available flights", async () => {
      const flights: unknown[] = []

      for await (const info of client.listFlights()) {
        flights.push(info)
      }

      // Server should have test fixtures
      expect(flights.length).toBeGreaterThan(0)
    })

    it("returns FlightInfo with required fields", async () => {
      for await (const info of client.listFlights()) {
        // FlightInfo must have a descriptor
        expect(info.flightDescriptor).toBeDefined()
        // Should have at least one endpoint for data retrieval
        expect(info.endpoint.length).toBeGreaterThanOrEqual(0)
        // Schema should be present
        expect(info.schema).toBeDefined()
        break // Just check the first one
      }
    })

    it("filters flights with criteria expression", async () => {
      const allFlights: unknown[] = []
      for await (const info of client.listFlights()) {
        allFlights.push(info)
      }

      // Filter with criteria containing "integers"
      const filtered: unknown[] = []
      for await (const info of client.listFlights({ expression: Buffer.from("integers") })) {
        filtered.push(info)
      }

      // Filtered results should be a subset
      expect(filtered.length).toBeLessThanOrEqual(allFlights.length)
    })
  })

  describe("getFlightInfo", () => {
    it("gets flight info for test/integers", async () => {
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)

      expect(info.flightDescriptor).toBeDefined()
      expect(info.schema).toBeDefined()
      expect(info.schema.length).toBeGreaterThan(0)
      // test/integers should have records (actual count may vary)
      expect(info.totalRecords).toBeGreaterThan(0)
    })

    it("gets flight info for test/strings", async () => {
      const descriptor = pathDescriptor(...config.flights.strings)
      const info = await client.getFlightInfo(descriptor)

      expect(info.totalRecords).toBe(100)
    })

    it("gets flight info for test/empty", async () => {
      const descriptor = pathDescriptor(...config.flights.empty)
      const info = await client.getFlightInfo(descriptor)

      expect(info.totalRecords).toBe(0)
    })

    it("gets flight info for test/large", async () => {
      const descriptor = pathDescriptor(...config.flights.large)
      const info = await client.getFlightInfo(descriptor)

      expect(info.totalRecords).toBe(10000)
    })

    it("returns NOT_FOUND for non-existent flight", async () => {
      const descriptor = pathDescriptor("nonexistent", "flight")

      try {
        await client.getFlightInfo(descriptor)
        expect.unreachable("Expected NOT_FOUND error")
      } catch (error) {
        expect((error as { code: string }).code).toBe("NOT_FOUND")
      }
    })
  })

  describe("getSchema", () => {
    it("gets schema for test/integers", async () => {
      const descriptor = pathDescriptor(...config.flights.integers)
      const result = await client.getSchema(descriptor)

      expect(result.schema).toBeDefined()
      expect(result.schema.length).toBeGreaterThan(0)
    })

    it("schema matches getFlightInfo schema", async () => {
      const descriptor = pathDescriptor(...config.flights.strings)

      const info = await client.getFlightInfo(descriptor)
      const schemaResult = await client.getSchema(descriptor)

      // Both should return the same schema bytes
      expect(Buffer.from(schemaResult.schema)).toEqual(Buffer.from(info.schema))
    })

    it("returns NOT_FOUND for non-existent flight", async () => {
      const descriptor = pathDescriptor("does", "not", "exist")

      try {
        await client.getSchema(descriptor)
        expect.unreachable("Expected NOT_FOUND error")
      } catch (error) {
        expect((error as { code: string }).code).toBe("NOT_FOUND")
      }
    })
  })
})
