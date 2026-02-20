import { describe, expect, it } from "bun:test"

import { createFlightClient, FlightClient } from "../../client"

describe("FlightClient", () => {
  describe("constructor", () => {
    it("initialises with minimal options", () => {
      const client = new FlightClient({ host: "localhost" })
      expect(client.state).toBe("disconnected")
      expect(client.isConnected).toBe(false)
    })

    it("computes address with default TLS port", () => {
      const client = new FlightClient({ host: "flight.example.com" })
      expect(client.address).toBe("flight.example.com:443")
    })

    it("computes address with default insecure port", () => {
      const client = new FlightClient({ host: "localhost", tls: false })
      expect(client.address).toBe("localhost:80")
    })

    it("computes address with custom port", () => {
      const client = new FlightClient({ host: "localhost", port: 8815 })
      expect(client.address).toBe("localhost:8815")
    })

    it("computes address with TLS options object", () => {
      const client = new FlightClient({
        host: "localhost",
        tls: { rootCerts: Buffer.from("fake-cert") }
      })
      // tls object is truthy, so it should use TLS port
      expect(client.address).toBe("localhost:443")
    })
  })

  describe("bearer token management", () => {
    it("starts with no bearer token", () => {
      const client = new FlightClient({ host: "localhost" })
      expect(client.getBearerToken()).toBeNull()
    })

    it("sets and gets bearer token", () => {
      const client = new FlightClient({ host: "localhost" })
      client.setBearerToken("test-token")
      expect(client.getBearerToken()).toBe("test-token")
    })

    it("clears bearer token", () => {
      const client = new FlightClient({ host: "localhost" })
      client.setBearerToken("test-token")
      client.clearBearerToken()
      expect(client.getBearerToken()).toBeNull()
    })
  })

  describe("createMetadata", () => {
    it("creates empty metadata when no auth configured", () => {
      const client = new FlightClient({ host: "localhost" })
      const metadata = client.createMetadata()
      expect(metadata.toJSON()).toEqual({})
    })

    it("includes bearer token when set", () => {
      const client = new FlightClient({ host: "localhost" })
      client.setBearerToken("my-token")
      const metadata = client.createMetadata()
      expect(metadata.get("authorization")).toEqual(["Bearer my-token"])
    })

    it("includes basic auth from options", () => {
      const client = new FlightClient({
        host: "localhost",
        auth: { type: "basic", username: "user", password: "pass" }
      })
      const metadata = client.createMetadata()
      const expected = Buffer.from("user:pass").toString("base64")
      expect(metadata.get("authorization")).toEqual([`Basic ${expected}`])
    })

    it("includes bearer auth from options", () => {
      const client = new FlightClient({
        host: "localhost",
        auth: { type: "bearer", token: "static-token" }
      })
      const metadata = client.createMetadata()
      expect(metadata.get("authorization")).toEqual(["Bearer static-token"])
    })

    it("includes custom headers from call options", () => {
      const client = new FlightClient({ host: "localhost" })
      const metadata = client.createMetadata({
        headers: { "x-custom-header": "custom-value" }
      })
      expect(metadata.get("x-custom-header")).toEqual(["custom-value"])
    })
  })

  describe("close", () => {
    it("transitions to closed state", () => {
      const client = new FlightClient({ host: "localhost" })
      client.close()
      expect(client.state).toBe("closed")
    })

    it("clears bearer token on close", () => {
      const client = new FlightClient({ host: "localhost" })
      client.setBearerToken("test-token")
      client.close()
      expect(client.getBearerToken()).toBeNull()
    })
  })

  describe("connect", () => {
    it("throws when client has been closed", async () => {
      const client = new FlightClient({ host: "localhost" })
      client.close()
      try {
        await client.connect()
        expect.unreachable("expected connect to throw")
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe("client has been closed")
      }
    })
  })

  describe("handshake", () => {
    it("throws when client is not connected", async () => {
      const client = new FlightClient({ host: "localhost" })
      try {
        await client.handshake()
        expect.unreachable("expected handshake to throw")
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe("client is not connected")
      }
    })
  })
})

describe("createFlightClient", () => {
  it("creates and attempts to connect", async () => {
    // This will fail to connect since there's no server, but it tests the function path
    try {
      await createFlightClient({
        host: "localhost",
        port: 19999,
        tls: false,
        channelOptions: { connectTimeoutMs: 100 }
      })
      expect.unreachable("expected createFlightClient to throw")
    } catch (error) {
      expect(error).toBeInstanceOf(Error)
    }
  })
})
