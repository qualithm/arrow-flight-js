/**
 * Integration tests for Flight client connection and handshake.
 *
 * Requires a running Arrow Flight server.
 *
 * @example
 * ```bash
 * # Start your Arrow Flight server, then run:
 * bun test src/__tests__/integration
 * ```
 */
import { afterEach, describe, expect, it } from "vitest"

import { createFlightClient, FlightClient } from "../../client"
import { config } from "./config"

describe("FlightClient Integration", () => {
  let client: FlightClient | null = null

  afterEach(() => {
    if (client !== null) {
      client.close()
      client = null
    }
  })

  describe("connect", () => {
    it("connects to the Arrow Flight server", async () => {
      client = new FlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls
      })

      expect(client.isConnected).toBe(false)
      expect(client.state).toBe("disconnected")

      await client.connect()

      expect(client.isConnected).toBe(true)
      expect(client.state).toBe("connected")
    })

    it("connects using createFlightClient helper", async () => {
      client = await createFlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls
      })

      expect(client.isConnected).toBe(true)
    })
  })

  describe("handshake", () => {
    it("performs basic auth handshake with valid credentials", async () => {
      client = new FlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls,
        auth: {
          type: "basic",
          username: config.credentials.admin.username,
          password: config.credentials.admin.password
        }
      })

      await client.connect()
      const result = await client.handshake()

      expect(result.protocolVersion).toBeGreaterThanOrEqual(0)
      // Server should return a bearer token
      expect(result.token).toBeDefined()
      expect(result.token?.length).toBeGreaterThan(0)
      // Token should be automatically set on client
      expect(client.getBearerToken()).toBe(result.token ?? null)
    })

    it("performs handshake with reader credentials", async () => {
      client = new FlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls,
        auth: {
          type: "basic",
          username: config.credentials.reader.username,
          password: config.credentials.reader.password
        }
      })

      await client.connect()
      const result = await client.handshake()

      expect(result.token).toBeDefined()
    })

    it("rejects invalid credentials", async () => {
      client = new FlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls,
        auth: {
          type: "basic",
          username: config.credentials.invalid.username,
          password: config.credentials.invalid.password
        }
      })

      await client.connect()

      try {
        await client.handshake()
        expect.unreachable("Expected handshake to fail")
      } catch (error) {
        expect((error as { code: string }).code).toBe("UNAUTHENTICATED")
      }
    })
  })

  describe("connection state", () => {
    it("transitions through connection states correctly", async () => {
      client = new FlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls
      })

      expect(client.state).toBe("disconnected")

      await client.connect()
      expect(client.state).toBe("connected")

      client.close()
      expect(client.state).toBe("closed")
    })

    it("reports correct address", () => {
      client = new FlightClient({
        host: config.host,
        port: config.port,
        tls: config.tls
      })

      expect(client.address).toBe(`${config.host}:${String(config.port)}`)
    })
  })
})
