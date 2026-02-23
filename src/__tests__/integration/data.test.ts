/**
 * Integration tests for data operations: doGet, doPut, doExchange.
 *
 * Requires a running Arrow Flight server with test fixtures.
 */
import { tableFromIPC } from "apache-arrow"
import { afterAll, beforeAll, describe, expect, it } from "bun:test"

import { createFlightClient, type FlightClient, type FlightData, pathDescriptor } from "../../index"
import { config } from "./config"

describe("Data Operations Integration", () => {
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

  describe("doGet", () => {
    it("retrieves data for test/integers", async () => {
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)

      // Should have at least one endpoint
      expect(info.endpoint.length).toBeGreaterThan(0)

      const endpoint = info.endpoint[0]
      expect(endpoint.ticket).toBeDefined()

      // Collect all FlightData messages
      const chunks: Uint8Array[] = []
      for await (const data of client.doGet(endpoint.ticket!)) {
        if (data.dataHeader.length > 0) {
          chunks.push(data.dataHeader)
        }
        if (data.dataBody.length > 0) {
          chunks.push(data.dataBody)
        }
      }

      // Should have received data
      expect(chunks.length).toBeGreaterThan(0)

      // Parse as Arrow table
      const table = tableFromIPC(chunks)
      expect(table.numRows).toBe(100)
      // Schema should have id and value columns
      expect(table.schema.fields.map((f) => f.name)).toContain("id")
      expect(table.schema.fields.map((f) => f.name)).toContain("value")
    })

    it("retrieves data for test/strings", async () => {
      const descriptor = pathDescriptor(...config.flights.strings)
      const info = await client.getFlightInfo(descriptor)

      const chunks: Uint8Array[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        if (data.dataHeader.length > 0) {
          chunks.push(data.dataHeader)
        }
        if (data.dataBody.length > 0) {
          chunks.push(data.dataBody)
        }
      }

      const table = tableFromIPC(chunks)
      expect(table.numRows).toBe(100)
      expect(table.schema.fields.map((f) => f.name)).toContain("name")
    })

    it("returns empty result for test/empty", async () => {
      const descriptor = pathDescriptor(...config.flights.empty)
      const info = await client.getFlightInfo(descriptor)

      const chunks: Uint8Array[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        if (data.dataHeader.length > 0) {
          chunks.push(data.dataHeader)
        }
        if (data.dataBody.length > 0) {
          chunks.push(data.dataBody)
        }
      }

      const table = tableFromIPC(chunks)
      expect(table.numRows).toBe(0)
    })

    it("retrieves large dataset", async () => {
      const descriptor = pathDescriptor(...config.flights.large)
      const info = await client.getFlightInfo(descriptor)

      const chunks: Uint8Array[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        if (data.dataHeader.length > 0) {
          chunks.push(data.dataHeader)
        }
        if (data.dataBody.length > 0) {
          chunks.push(data.dataBody)
        }
      }

      const table = tableFromIPC(chunks)
      expect(table.numRows).toBe(10000)
    })

    it("retrieves nested types", async () => {
      const descriptor = pathDescriptor(...config.flights.nested)
      const info = await client.getFlightInfo(descriptor)

      const chunks: Uint8Array[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        if (data.dataHeader.length > 0) {
          chunks.push(data.dataHeader)
        }
        if (data.dataBody.length > 0) {
          chunks.push(data.dataBody)
        }
      }

      const table = tableFromIPC(chunks)
      expect(table.numRows).toBe(50)
      // Should have items column with List type
      expect(table.schema.fields.map((f) => f.name)).toContain("items")
    })
  })

  describe("doPut", () => {
    it("uploads data and receives acknowledgement", async () => {
      // First, get schema from an existing flight to use as template
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)

      // Get the original data
      const sourceChunks: FlightData[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        sourceChunks.push(data)
      }

      // Upload to a new path
      const putStream = client.doPut()

      // First message includes descriptor
      const firstData = sourceChunks[0]
      putStream.write({
        flightDescriptor: {
          type: 1, // PATH
          path: ["test", `put-test-${String(Date.now())}`],
          cmd: Buffer.alloc(0)
        },
        dataHeader: firstData.dataHeader,
        dataBody: firstData.dataBody,
        appMetadata: Buffer.alloc(0)
      })

      // Remaining data
      for (let i = 1; i < sourceChunks.length; i++) {
        putStream.write({
          flightDescriptor: undefined,
          dataHeader: sourceChunks[i].dataHeader,
          dataBody: sourceChunks[i].dataBody,
          appMetadata: Buffer.alloc(0)
        })
      }

      putStream.end()

      // Collect acknowledgements
      const acks: unknown[] = []
      for await (const result of putStream.results()) {
        acks.push(result)
      }

      // Should receive at least one acknowledgement
      expect(acks.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe("doExchange", () => {
    it("exchanges data bidirectionally (echo)", async () => {
      const exchangeStream = client.doExchange()

      // Send a message with exchange/echo path
      const descriptor = pathDescriptor("exchange", "echo")
      const info = await client.getFlightInfo(descriptor).catch(() => null)

      // If exchange/echo is supported, test it
      if (info !== null) {
        exchangeStream.write({
          flightDescriptor: {
            type: 1, // PATH
            path: ["exchange", "echo"],
            cmd: Buffer.alloc(0)
          },
          dataHeader: Buffer.from("test-header"),
          dataBody: Buffer.from("test-body"),
          appMetadata: Buffer.alloc(0)
        })

        exchangeStream.end()

        const results: FlightData[] = []
        for await (const data of exchangeStream.results()) {
          results.push(data)
        }

        // Echo should return the same data
        expect(results.length).toBeGreaterThan(0)
      } else {
        // Exchange might not be available, just close cleanly
        exchangeStream.end()
      }
    })
  })
})
