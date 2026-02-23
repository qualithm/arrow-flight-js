/**
 * Integration tests for authorization and permissions.
 *
 * Requires a running Arrow Flight server with test users.
 */
import { afterEach, describe, expect, it } from "bun:test"

import { createFlightClient, type FlightClient, type FlightData, pathDescriptor } from "../../index"
import { config } from "./config"

describe("Authorization Integration", () => {
  let client: FlightClient | null = null

  afterEach(() => {
    if (client !== null) {
      client.close()
      client = null
    }
  })

  describe("read-only user", () => {
    it("can read data", async () => {
      client = await createFlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls,
        auth: {
          type: "basic",
          username: config.credentials.reader.username,
          password: config.credentials.reader.password
        }
      })
      await client.handshake()

      // Reader should be able to list flights
      const flights: unknown[] = []
      for await (const info of client.listFlights()) {
        flights.push(info)
      }
      expect(flights.length).toBeGreaterThan(0)

      // Reader should be able to get flight info
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)
      expect(info).toBeDefined()

      // Reader should be able to doGet
      const chunks: Uint8Array[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        if (data.dataHeader.length > 0) {
          chunks.push(data.dataHeader)
        }
        if (data.dataBody.length > 0) {
          chunks.push(data.dataBody)
        }
      }
      expect(chunks.length).toBeGreaterThan(0)
    })

    it("cannot write data (PERMISSION_DENIED)", async () => {
      client = await createFlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls,
        auth: {
          type: "basic",
          username: config.credentials.reader.username,
          password: config.credentials.reader.password
        }
      })
      await client.handshake()

      // Get source data
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)
      const sourceChunks: FlightData[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        sourceChunks.push(data)
      }

      // Try to put data - should fail
      const putStream = client.doPut()

      putStream.write({
        flightDescriptor: {
          type: 1,
          path: ["test", "unauthorized-write"],
          cmd: Buffer.alloc(0)
        },
        dataHeader: sourceChunks[0]?.dataHeader ?? Buffer.alloc(0),
        dataBody: sourceChunks[0]?.dataBody ?? Buffer.alloc(0),
        appMetadata: Buffer.alloc(0)
      })

      putStream.end()

      // Collecting results should fail with PERMISSION_DENIED
      try {
        for await (const _ of putStream.results()) {
          // Should not succeed
        }
        expect.unreachable("Expected PERMISSION_DENIED error")
      } catch (error) {
        expect((error as { code: string }).code).toBe("PERMISSION_DENIED")
      }
    })
  })

  describe("admin user", () => {
    it("can both read and write data", async () => {
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

      // Admin should be able to read
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)

      const sourceChunks: FlightData[] = []
      for await (const data of client.doGet(info.endpoint[0].ticket!)) {
        sourceChunks.push(data)
      }

      expect(sourceChunks.length).toBeGreaterThan(0)

      // Admin should be able to write
      const putStream = client.doPut()

      putStream.write({
        flightDescriptor: {
          type: 1,
          path: ["test", `admin-write-${String(Date.now())}`],
          cmd: Buffer.alloc(0)
        },
        dataHeader: sourceChunks[0]?.dataHeader ?? Buffer.alloc(0),
        dataBody: sourceChunks[0]?.dataBody ?? Buffer.alloc(0),
        appMetadata: Buffer.alloc(0)
      })

      putStream.end()

      // Should succeed without error
      const acks: unknown[] = []
      for await (const result of putStream.results()) {
        acks.push(result)
      }

      // Write succeeded without PERMISSION_DENIED
      expect(true).toBe(true)
    })
  })

  describe("bearer token authentication", () => {
    it("works with token from handshake", async () => {
      // First client does handshake
      const authClient = await createFlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls,
        auth: {
          type: "basic",
          username: config.credentials.admin.username,
          password: config.credentials.admin.password
        }
      })
      const handshakeResult = await authClient.handshake()
      const { token } = handshakeResult
      authClient.close()

      expect(token).toBeDefined()

      // Second client uses bearer token
      client = await createFlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls,
        auth: {
          type: "bearer",
          token: token!
        }
      })

      // Should be able to make authenticated requests
      const flights: unknown[] = []
      for await (const info of client.listFlights()) {
        flights.push(info)
      }
      expect(flights.length).toBeGreaterThan(0)
    })

    it("can manually set bearer token", async () => {
      // Get token via handshake
      const authClient = await createFlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls,
        auth: {
          type: "basic",
          username: config.credentials.admin.username,
          password: config.credentials.admin.password
        }
      })
      const { token } = await authClient.handshake()
      authClient.close()

      // New client without auth, manually set token
      client = await createFlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls
      })

      client.setBearerToken(token!)

      // Should work
      const descriptor = pathDescriptor(...config.flights.integers)
      const info = await client.getFlightInfo(descriptor)
      expect(info).toBeDefined()
    })
  })
})
