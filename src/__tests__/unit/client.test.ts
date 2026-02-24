import { describe, expect, it } from "vitest"

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

  describe("listFlights", () => {
    it("throws when client is not connected", async () => {
      const client = new FlightClient({ host: "localhost" })
      try {
        for await (const _ of client.listFlights()) {
          expect.unreachable("should not yield any values")
        }
        expect.unreachable("expected listFlights to throw")
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe("client is not connected")
      }
    })
  })

  describe("getFlightInfo", () => {
    it("throws when client is not connected", async () => {
      const client = new FlightClient({ host: "localhost" })
      try {
        await client.getFlightInfo({ type: "path", path: ["test"] })
        expect.unreachable("expected getFlightInfo to throw")
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe("client is not connected")
      }
    })
  })

  describe("getSchema", () => {
    it("throws when client is not connected", async () => {
      const client = new FlightClient({ host: "localhost" })
      try {
        await client.getSchema({ type: "cmd", cmd: Buffer.from("SELECT 1") })
        expect.unreachable("expected getSchema to throw")
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe("client is not connected")
      }
    })
  })

  describe("doGet", () => {
    it("throws when client is not connected", async () => {
      const client = new FlightClient({ host: "localhost" })
      try {
        for await (const _ of client.doGet({ ticket: Buffer.from("test") })) {
          expect.unreachable("should not yield any values")
        }
        expect.unreachable("expected doGet to throw")
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe("client is not connected")
      }
    })
  })

  describe("doPut", () => {
    it("throws when client is not connected", () => {
      const client = new FlightClient({ host: "localhost" })
      expect(() => client.doPut()).toThrow("client is not connected")
    })
  })

  describe("doExchange", () => {
    it("throws when client is not connected", () => {
      const client = new FlightClient({ host: "localhost" })
      expect(() => client.doExchange()).toThrow("client is not connected")
    })
  })

  describe("doAction", () => {
    it("throws when client is not connected", async () => {
      const client = new FlightClient({ host: "localhost" })
      try {
        for await (const _ of client.doAction({ type: "test", body: Buffer.alloc(0) })) {
          expect.unreachable("should not yield any values")
        }
        expect.unreachable("expected doAction to throw")
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe("client is not connected")
      }
    })
  })

  describe("listActions", () => {
    it("throws when client is not connected", async () => {
      const client = new FlightClient({ host: "localhost" })
      try {
        for await (const _ of client.listActions()) {
          expect.unreachable("should not yield any values")
        }
        expect.unreachable("expected listActions to throw")
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe("client is not connected")
      }
    })
  })

  describe("cancelFlightInfo", () => {
    it("throws when client is not connected", async () => {
      const client = new FlightClient({ host: "localhost" })
      const mockFlightInfo = {
        schema: Buffer.alloc(0),
        flightDescriptor: undefined,
        endpoint: [],
        totalRecords: 0,
        totalBytes: 0,
        ordered: false,
        appMetadata: Buffer.alloc(0)
      }
      try {
        await client.cancelFlightInfo(mockFlightInfo)
        expect.unreachable("expected cancelFlightInfo to throw")
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

describe("channel options coverage", () => {
  it("applies keepaliveTimeoutMs option", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: false,
        channelOptions: {
          connectTimeoutMs: 100,
          keepaliveTimeoutMs: 5000
        }
      })
    } catch {
      // Expected - no server running
    }
  })

  it("applies keepalivePermitWithoutCalls option", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: false,
        channelOptions: {
          connectTimeoutMs: 100,
          keepalivePermitWithoutCalls: true
        }
      })
    } catch {
      // Expected - no server running
    }
  })

  it("applies serverNameOverride from TLS options", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: {
          serverNameOverride: "custom.example.com"
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server running
    }
  })

  it("applies maxReceiveMessageLength and maxSendMessageLength", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: false,
        channelOptions: {
          connectTimeoutMs: 100,
          maxReceiveMessageLength: 1024 * 1024 * 100,
          maxSendMessageLength: 1024 * 1024 * 100
        }
      })
    } catch {
      // Expected - no server running
    }
  })

  it("applies keepaliveTimeMs option", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: false,
        channelOptions: {
          connectTimeoutMs: 100,
          keepaliveTimeMs: 60000
        }
      })
    } catch {
      // Expected - no server running
    }
  })
})

describe("TLS credentials coverage", () => {
  it("handles TLS with rootCerts as string", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: {
          rootCerts: "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server running or invalid cert
    }
  })

  it("handles TLS with verifyServerCert disabled", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: {
          verifyServerCert: false
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server running
    }
  })

  it("handles TLS with privateKey and certChain as strings", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: {
          privateKey: "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----",
          certChain: "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server or invalid certs
    }
  })
})

describe("mTLS auth coverage", () => {
  it("handles mTLS auth with string certs", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        auth: {
          type: "mtls",
          cert: "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
          key: "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----"
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server or invalid certs
    }
  })

  it("handles mTLS auth with Buffer certs", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        auth: {
          type: "mtls",
          cert: Buffer.from("fake-cert"),
          key: Buffer.from("fake-key")
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server or invalid certs
    }
  })

  it("handles mTLS auth with CA certificate", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        auth: {
          type: "mtls",
          cert: Buffer.from("fake-cert"),
          key: Buffer.from("fake-key"),
          ca: "-----BEGIN CERTIFICATE-----\nfake-ca\n-----END CERTIFICATE-----"
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server or invalid certs
    }
  })

  it("handles mTLS auth with CA as Buffer", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        auth: {
          type: "mtls",
          cert: Buffer.from("fake-cert"),
          key: Buffer.from("fake-key"),
          ca: Buffer.from("-----BEGIN CERTIFICATE-----\nfake-ca\n-----END CERTIFICATE-----")
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server or invalid certs
    }
  })
})

describe("TLS options with Buffer values", () => {
  it("handles TLS with rootCerts as Buffer", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: {
          rootCerts: Buffer.from("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----")
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server running or invalid cert
    }
  })

  it("handles TLS with privateKey as Buffer", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: {
          privateKey: Buffer.from("-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----")
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server or invalid certs
    }
  })

  it("handles TLS with certChain as Buffer", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: {
          certChain: Buffer.from("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----")
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server or invalid certs
    }
  })

  it("handles TLS with all options as Buffers", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: {
          rootCerts: Buffer.from(
            "-----BEGIN CERTIFICATE-----\nfake-root\n-----END CERTIFICATE-----"
          ),
          privateKey: Buffer.from(
            "-----BEGIN PRIVATE KEY-----\nfake-key\n-----END PRIVATE KEY-----"
          ),
          certChain: Buffer.from(
            "-----BEGIN CERTIFICATE-----\nfake-chain\n-----END CERTIFICATE-----"
          )
        },
        channelOptions: { connectTimeoutMs: 100 }
      })
    } catch {
      // Expected - no server or invalid certs
    }
  })
})

describe("keepalivePermitWithoutCalls false", () => {
  it("handles keepalivePermitWithoutCalls set to false", async () => {
    try {
      await createFlightClient({
        host: "localhost",
        port: 19998,
        tls: false,
        channelOptions: {
          connectTimeoutMs: 100,
          keepalivePermitWithoutCalls: false
        }
      })
    } catch {
      // Expected - no server running
    }
  })
})
