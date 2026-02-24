/**
 * Mock tests for client error paths that require simulated server responses.
 */
import { type ChannelCredentials, Metadata } from "@grpc/grpc-js"
import { EventEmitter } from "events"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { FlightClient, FlightError } from "../../index"

// Use vi.hoisted to ensure credentialCalls is available when mock is hoisted
const credentialCalls = vi.hoisted(() => ({
  createInsecure: 0,
  createSsl: 0,
  lastSslVerifyOptions: null as { checkServerIdentity?: () => undefined } | null
}))

// Mock the gRPC client
vi.mock("@grpc/grpc-js", async () => {
  const actual = await vi.importActual("@grpc/grpc-js")
  return {
    ...actual,
    credentials: {
      createInsecure: vi.fn(() => {
        credentialCalls.createInsecure++
        return {}
      }),
      createSsl: vi.fn(
        (
          _rootCerts?: Buffer,
          _privateKey?: Buffer,
          _certChain?: Buffer,
          verifyOptions?: { checkServerIdentity?: () => undefined }
        ) => {
          credentialCalls.createSsl++
          credentialCalls.lastSslVerifyOptions = verifyOptions ?? null
          return {}
        }
      )
    }
  }
})

// Mock the generated FlightServiceClient
vi.mock("../../generated/arrow/flight/protocol/Flight.js", async () => {
  const actual = await vi.importActual("../../generated/arrow/flight/protocol/Flight.js")

  // Create a proper constructor function
  class MockFlightServiceClient {
    waitForReady = vi.fn((_, cb: (err?: Error) => void) => {
      cb()
    })
    close = vi.fn()
  }

  return {
    ...actual,
    FlightServiceClient: MockFlightServiceClient
  }
})

describe("DoExchangeStream error handling", () => {
  let client: FlightClient
  let mockStream: EventEmitter & {
    write: ReturnType<typeof vi.fn>
    end: ReturnType<typeof vi.fn>
    cancel: ReturnType<typeof vi.fn>
  }

  beforeEach(() => {
    // Create a mock stream
    mockStream = Object.assign(new EventEmitter(), {
      write: vi.fn(() => true),
      end: vi.fn(),
      cancel: vi.fn()
    })

    client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    // Mock the internal gRPC client
    const mockGrpcClient = {
      doExchange: vi.fn(() => mockStream),
      waitForReady: vi.fn((_, cb) => cb())
    }
    // @ts-expect-error accessing private property for testing
    client.grpcClient = mockGrpcClient
    // @ts-expect-error accessing private property for testing
    client._state = "connected"
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("handles stream error events", async () => {
    const exchangeStream = client.doExchange()

    // Write some data
    exchangeStream.write({
      flightDescriptor: undefined,
      dataHeader: Buffer.from("test"),
      dataBody: Buffer.from("data"),
      appMetadata: Buffer.alloc(0)
    })

    // Emit error from server
    const serverError = new Error("Server error")
    // @ts-expect-error - add gRPC error properties
    serverError.code = 13 // INTERNAL
    // @ts-expect-error - add gRPC error properties
    serverError.details = "Internal server error"

    setTimeout(() => {
      mockStream.emit("error", serverError)
    }, 10)

    // Collect results should throw
    await expect(exchangeStream.collectResults()).rejects.toThrow()
  })

  it("handles stream end events normally", async () => {
    const exchangeStream = client.doExchange()

    // Emit data then end
    setTimeout(() => {
      mockStream.emit("data", {
        flightDescriptor: undefined,
        dataHeader: Buffer.from("response"),
        dataBody: Buffer.from("data"),
        appMetadata: Buffer.alloc(0)
      })
      mockStream.emit("end")
    }, 10)

    const results = await exchangeStream.collectResults()
    expect(results).toHaveLength(1)
  })
})

describe("DoPutStream error handling", () => {
  let client: FlightClient
  let mockStream: EventEmitter & {
    write: ReturnType<typeof vi.fn>
    end: ReturnType<typeof vi.fn>
    cancel: ReturnType<typeof vi.fn>
  }

  beforeEach(() => {
    mockStream = Object.assign(new EventEmitter(), {
      write: vi.fn(() => true),
      end: vi.fn(),
      cancel: vi.fn()
    })

    client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    const mockGrpcClient = {
      doPut: vi.fn(() => mockStream),
      waitForReady: vi.fn((_, cb) => cb())
    }
    // @ts-expect-error accessing private property for testing
    client.grpcClient = mockGrpcClient
    // @ts-expect-error accessing private property for testing
    client._state = "connected"
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("handles put stream error events", async () => {
    const putStream = client.doPut()

    putStream.write({
      flightDescriptor: {
        type: 1,
        path: ["test"],
        cmd: Buffer.alloc(0)
      },
      dataHeader: Buffer.from("test"),
      dataBody: Buffer.from("data"),
      appMetadata: Buffer.alloc(0)
    })

    putStream.end()

    const serverError = new Error("Put failed")
    setTimeout(() => {
      mockStream.emit("error", serverError)
    }, 10)

    await expect(putStream.collectResults()).rejects.toThrow("Put failed")
  })

  it("handles put stream end with results", async () => {
    const putStream = client.doPut()

    putStream.write({
      flightDescriptor: {
        type: 1,
        path: ["test"],
        cmd: Buffer.alloc(0)
      },
      dataHeader: Buffer.from("test"),
      dataBody: Buffer.from("data"),
      appMetadata: Buffer.alloc(0)
    })

    putStream.end()

    setTimeout(() => {
      mockStream.emit("data", { appMetadata: Buffer.from("ack") })
      mockStream.emit("end")
    }, 10)

    const results = await putStream.collectResults()
    expect(results).toHaveLength(1)
  })
})

describe("FlightError with Buffer metadata", () => {
  it("handles gRPC errors with Buffer metadata values", () => {
    const metadata = new Metadata()
    // Add a string value
    metadata.set("string-key", "string-value")
    // Add a Buffer value (binary metadata)
    metadata.set("binary-key-bin", Buffer.from([0x01, 0x02, 0x03]))

    const grpcError = {
      code: 13,
      message: "Internal error",
      details: "Internal error",
      metadata
    }

    const flightError = FlightError.fromGrpcError(grpcError)
    expect(flightError).toBeInstanceOf(FlightError)
    expect(flightError.code).toBe("INTERNAL")
    // The string key should be present, buffer key is skipped
    expect(flightError.metadata?.["string-key"]).toBe("string-value")
    // Buffer values are not included (extractGrpcMetadata skips them)
    expect(flightError.metadata?.["binary-key-bin"]).toBeUndefined()
  })

  it("handles gRPC errors with only Buffer metadata", () => {
    const metadata = new Metadata()
    metadata.set("only-binary-bin", Buffer.from([0x01, 0x02]))

    const grpcError = {
      code: 3,
      message: "Invalid argument",
      details: "Invalid argument",
      metadata
    }

    const flightError = FlightError.fromGrpcError(grpcError)
    expect(flightError).toBeInstanceOf(FlightError)
    expect(flightError.code).toBe("INVALID_ARGUMENT")
    // No string metadata, so metadata should be undefined
    expect(flightError.metadata).toBeUndefined()
  })

  it("handles gRPC errors with no metadata", () => {
    const grpcError = {
      code: 5,
      message: "Not found",
      details: "Not found"
    }

    const flightError = FlightError.fromGrpcError(grpcError)
    expect(flightError).toBeInstanceOf(FlightError)
    expect(flightError.code).toBe("NOT_FOUND")
    expect(flightError.metadata).toBeUndefined()
  })

  it("handles unknown gRPC status codes", () => {
    const grpcError = {
      code: 999, // Unknown code
      message: "Unknown error",
      details: "Unknown error"
    }

    const flightError = FlightError.fromGrpcError(grpcError)
    expect(flightError).toBeInstanceOf(FlightError)
    expect(flightError.code).toBe("UNKNOWN")
  })

  it("wraps non-gRPC errors", () => {
    const plainError = new Error("Something went wrong")
    const flightError = FlightError.fromGrpcError(plainError)
    expect(flightError).toBeInstanceOf(FlightError)
    expect(flightError.code).toBe("UNKNOWN")
    expect(flightError.message).toBe("Something went wrong")
  })
})

describe("DoPutStream additional methods", () => {
  let client: FlightClient
  let mockStream: EventEmitter & {
    write: ReturnType<typeof vi.fn>
    end: ReturnType<typeof vi.fn>
    cancel: ReturnType<typeof vi.fn>
  }

  beforeEach(() => {
    mockStream = Object.assign(new EventEmitter(), {
      write: vi.fn(() => true),
      end: vi.fn(),
      cancel: vi.fn()
    })

    client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    const mockGrpcClient = {
      doPut: vi.fn(() => mockStream),
      waitForReady: vi.fn((_, cb) => cb())
    }
    // @ts-expect-error accessing private property for testing
    client.grpcClient = mockGrpcClient
    // @ts-expect-error accessing private property for testing
    client._state = "connected"
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("calls cancel on the underlying stream", () => {
    const putStream = client.doPut()
    putStream.cancel()
    expect(mockStream.cancel).toHaveBeenCalled()
  })

  it("iterates results using results() generator directly", async () => {
    const putStream = client.doPut()

    setTimeout(() => {
      mockStream.emit("data", { appMetadata: Buffer.from("result1") })
      mockStream.emit("data", { appMetadata: Buffer.from("result2") })
      mockStream.emit("end")
    }, 5)

    const results: unknown[] = []
    for await (const result of putStream.results()) {
      results.push(result)
    }

    expect(results).toHaveLength(2)
  })
})

describe("DoExchangeStream additional methods", () => {
  let client: FlightClient
  let mockStream: EventEmitter & {
    write: ReturnType<typeof vi.fn>
    end: ReturnType<typeof vi.fn>
    cancel: ReturnType<typeof vi.fn>
  }

  beforeEach(() => {
    mockStream = Object.assign(new EventEmitter(), {
      write: vi.fn(() => true),
      end: vi.fn(),
      cancel: vi.fn()
    })

    client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    const mockGrpcClient = {
      doExchange: vi.fn(() => mockStream),
      waitForReady: vi.fn((_, cb) => cb())
    }
    // @ts-expect-error accessing private property for testing
    client.grpcClient = mockGrpcClient
    // @ts-expect-error accessing private property for testing
    client._state = "connected"
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("calls end on the underlying stream", () => {
    const exchangeStream = client.doExchange()
    exchangeStream.end()
    expect(mockStream.end).toHaveBeenCalled()
  })

  it("iterates results using results() generator directly", async () => {
    const exchangeStream = client.doExchange()

    setTimeout(() => {
      mockStream.emit("data", {
        flightDescriptor: undefined,
        dataHeader: Buffer.from("header1"),
        dataBody: Buffer.from("body1"),
        appMetadata: Buffer.alloc(0)
      })
      mockStream.emit("data", {
        flightDescriptor: undefined,
        dataHeader: Buffer.from("header2"),
        dataBody: Buffer.from("body2"),
        appMetadata: Buffer.alloc(0)
      })
      mockStream.emit("end")
    }, 5)

    const results: unknown[] = []
    for await (const result of exchangeStream.results()) {
      results.push(result)
    }

    expect(results).toHaveLength(2)
  })
})

describe("TLS credential paths", () => {
  beforeEach(() => {
    credentialCalls.createInsecure = 0
    credentialCalls.createSsl = 0
  })

  it("uses default SSL credentials when tls is true", async () => {
    const client = new FlightClient({ host: "tls.example.com", tls: true })

    // Connect should exercise the createSsl() path
    await client.connect()

    expect(client.isConnected).toBe(true)
    expect(credentialCalls.createSsl).toBeGreaterThan(0)
  })

  it("uses default SSL credentials when tls is undefined", async () => {
    const client = new FlightClient({ host: "tls.example.com" })

    // Connect should exercise the createSsl() path (default)
    await client.connect()

    expect(client.isConnected).toBe(true)
    expect(credentialCalls.createSsl).toBeGreaterThan(0)
  })
})

describe("waitForReady edge cases", () => {
  it("rejects when grpcClient is null during connect", async () => {
    const client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    // Force grpcClient to null before connect completes
    // @ts-expect-error accessing private property for testing
    client.grpcClient = null
    // @ts-expect-error accessing private method for testing
    await expect(client.waitForReady()).rejects.toThrow("client not initialised")
  })
})

describe("gRPC error edge cases", () => {
  it("uses message when details is empty", () => {
    const grpcError = {
      code: 13,
      message: "The real error message",
      details: "", // Empty details
      metadata: undefined
    }

    const flightError = FlightError.fromGrpcError(grpcError)
    expect(flightError.message).toBe("The real error message")
  })

  it("handles non-Error objects", () => {
    const flightError = FlightError.fromGrpcError("string error")
    expect(flightError.message).toBe("string error")
    expect(flightError.code).toBe("UNKNOWN")
  })
})

describe("cancelFlightInfo edge cases", () => {
  let client: FlightClient

  beforeEach(() => {
    client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    const mockGrpcClient = {
      waitForReady: vi.fn((_, cb) => cb())
    }
    // @ts-expect-error accessing private property for testing
    client.grpcClient = mockGrpcClient
    // @ts-expect-error accessing private property for testing
    client._state = "connected"
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("returns unspecified when no result returned", async () => {
    // Mock doAction to yield nothing (empty generator)
    vi.spyOn(client, "doAction").mockImplementation(async function* () {
      // Empty generator - no results
    })

    const flightInfo = {
      schema: Buffer.alloc(0),
      flightDescriptor: { type: 1, path: ["test"], cmd: Buffer.alloc(0) },
      endpoint: [],
      totalRecords: 0,
      totalBytes: 0,
      ordered: false,
      appMetadata: Buffer.alloc(0)
    }

    const result = await client.cancelFlightInfo(flightInfo)
    expect(result).toBe("unspecified")
  })

  it("returns cancelled status when result returned", async () => {
    // Import the encoder for CancelFlightInfoResult
    const { CancelFlightInfoResult } =
      await import("../../generated/arrow/flight/protocol/Flight.js")

    // Create a mock result with CANCELLED status (value 1)
    const cancelResult = CancelFlightInfoResult.encode({ status: 1 }).finish()

    // Mock doAction to yield the result
    // eslint-disable-next-line @typescript-eslint/require-await
    vi.spyOn(client, "doAction").mockImplementation(async function* () {
      yield { body: Buffer.from(cancelResult) }
    })

    const flightInfo = {
      schema: Buffer.alloc(0),
      flightDescriptor: { type: 1, path: ["test"], cmd: Buffer.alloc(0) },
      endpoint: [],
      totalRecords: 0,
      totalBytes: 0,
      ordered: false,
      appMetadata: Buffer.alloc(0)
    }

    const result = await client.cancelFlightInfo(flightInfo)
    expect(result).toBe("cancelled")
  })
})

describe("auth payload paths", () => {
  beforeEach(() => {
    credentialCalls.createInsecure = 0
    credentialCalls.createSsl = 0
  })

  it("creates handshake payload from handshake auth type", async () => {
    const client = new FlightClient({
      host: "localhost",
      port: 8815,
      tls: false,
      auth: {
        type: "handshake",
        payload: Buffer.from("custom-handshake")
      }
    })

    await client.connect()

    // Mock the grpcClient.handshake method
    const mockHandshakeStream = new EventEmitter()
    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    // Call handshake to exercise the payload building
    const result = await client.handshake()
    expect(result).toBeDefined()
  })

  it("creates empty payload when no auth specified", async () => {
    const client = new FlightClient({
      host: "localhost",
      port: 8815,
      tls: false
      // No auth specified
    })

    await client.connect()

    // Mock the grpcClient.handshake method
    const mockHandshakeStream = new EventEmitter()
    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    // Call handshake to exercise the payload building
    const result = await client.handshake()
    expect(result).toBeDefined()
  })
})

describe("explicit credentials path", () => {
  it("uses explicit credentials when provided", async () => {
    // Create a mock ChannelCredentials object
    const mockCredentials = {
      _credentials: "mock"
    } as unknown as ChannelCredentials

    const client = new FlightClient({
      host: "localhost",
      port: 8815,
      credentials: mockCredentials
    })

    await client.connect()
    expect(client.isConnected).toBe(true)
  })
})

describe("connect edge cases", () => {
  it("returns early when already connected", async () => {
    const client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    // First connect
    await client.connect()
    expect(client.isConnected).toBe(true)

    // Second connect should return early
    await client.connect()
    expect(client.isConnected).toBe(true)
  })
})

describe("handshake token extraction", () => {
  let client: FlightClient

  beforeEach(async () => {
    client = new FlightClient({ host: "localhost", port: 8815, tls: false })
    await client.connect()
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("extracts token from auth-token-bin metadata", async () => {
    const mockHandshakeStream = new EventEmitter()
    const mockMetadata = new Metadata()
    mockMetadata.set("auth-token-bin", Buffer.from("binary-token"))

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("metadata", mockMetadata)
        mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    const result = await client.handshake()
    expect(result).toBeDefined()
    expect(client.getBearerToken()).toBe("binary-token")
  })

  it("extracts token from response payload", async () => {
    const mockHandshakeStream = new EventEmitter()

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("data", {
          protocolVersion: 0,
          payload: Buffer.from("payload-token")
        })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    const result = await client.handshake()
    expect(result).toBeDefined()
    expect(client.getBearerToken()).toBe("payload-token")
  })

  it("handles string value in auth-token-bin metadata", async () => {
    const mockHandshakeStream = new EventEmitter()
    // Create a mock metadata object that returns a string for auth-token-bin
    // This tests the defensive typeof === "string" branch
    const mockMetadata = {
      get: (key: string) => {
        if (key === "auth-token-bin") {
          return ["string-token-value"] // String value (unusual but handled)
        }
        if (key === "authorization") {
          return []
        }
        return []
      }
    }

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("metadata", mockMetadata)
        mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    const result = await client.handshake()
    expect(result).toBeDefined()
    expect(client.getBearerToken()).toBe("string-token-value")
  })

  it("ignores non-bearer authorization headers", async () => {
    // Clear any previous token
    client.clearBearerToken()

    const mockHandshakeStream = new EventEmitter()
    // Create a mock metadata object with non-bearer auth
    const mockMetadata = {
      get: (key: string) => {
        if (key === "authorization") {
          return ["Basic dXNlcjpwYXNz"] // Basic auth, not bearer
        }
        if (key === "auth-token-bin") {
          return []
        }
        return []
      }
    }

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("metadata", mockMetadata)
        mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    const result = await client.handshake()
    expect(result).toBeDefined()
    // Token should not be set since auth header was not bearer
    expect(client.getBearerToken()).toBeNull()
  })

  it("extracts Buffer token from auth-token-bin when authorization header is missing", async () => {
    // Clear any previous token
    client.clearBearerToken()

    const mockHandshakeStream = new EventEmitter()
    // Create a Buffer that we can verify is truly a Buffer
    const tokenBuffer = Buffer.from("buffer-token-value")
    // Sanity check - ensure this is a real Buffer
    expect(typeof tokenBuffer).toBe("object")
    expect(Buffer.isBuffer(tokenBuffer)).toBe(true)

    // Create a mock metadata object that returns a Buffer for auth-token-bin
    const mockMetadata = {
      get: (key: string) => {
        if (key === "auth-token-bin") {
          // Return a genuine Buffer, not a string
          return [tokenBuffer]
        }
        if (key === "authorization") {
          return []
        }
        return []
      }
    }

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("metadata", mockMetadata)
        mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    const result = await client.handshake()
    expect(result).toBeDefined()
    // Should extract the token from the Buffer
    expect(client.getBearerToken()).toBe("buffer-token-value")
  })
})

describe("TLS verifyServerCert callback coverage", () => {
  beforeEach(() => {
    credentialCalls.createInsecure = 0
    credentialCalls.createSsl = 0
    credentialCalls.lastSslVerifyOptions = null
  })

  it("invokes checkServerIdentity callback when verifyServerCert is false", async () => {
    // Create client with TLS and verifyServerCert disabled
    const client = new FlightClient({
      host: "localhost",
      port: 8815,
      tls: {
        rootCerts: Buffer.from("fake-cert"),
        verifyServerCert: false
      }
    })

    // Connect to trigger credential building
    await client.connect()

    // The createSsl should have been called with verifyOptions containing checkServerIdentity
    expect(credentialCalls.createSsl).toBeGreaterThanOrEqual(1)
    expect(credentialCalls.lastSslVerifyOptions).not.toBeNull()
    expect(credentialCalls.lastSslVerifyOptions?.checkServerIdentity).toBeDefined()

    // Invoke the checkServerIdentity callback to get 100% function coverage
    credentialCalls.lastSslVerifyOptions?.checkServerIdentity?.()
    // Callback should return undefined (no-op)
    expect(typeof credentialCalls.lastSslVerifyOptions?.checkServerIdentity).toBe("function")

    client.close()
  })

  it("does not set checkServerIdentity when verifyServerCert is true", async () => {
    // Create client with TLS and verifyServerCert enabled (default)
    const client = new FlightClient({
      host: "localhost",
      port: 8815,
      tls: {
        rootCerts: Buffer.from("fake-cert"),
        verifyServerCert: true
      }
    })

    // Connect to trigger credential building
    await client.connect()

    // The createSsl should have been called without verifyOptions
    expect(credentialCalls.createSsl).toBeGreaterThanOrEqual(1)
    expect(credentialCalls.lastSslVerifyOptions).toBeNull()

    client.close()
  })
})

describe("handshake bearer token from authorization header", () => {
  let client: FlightClient

  beforeEach(async () => {
    client = new FlightClient({ host: "localhost", port: 8815, tls: false })
    await client.connect()
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("extracts token from bearer authorization header", async () => {
    client.clearBearerToken()

    const mockHandshakeStream = new EventEmitter()
    const mockMetadata = {
      get: (key: string) => {
        if (key === "authorization") {
          return ["Bearer my-extracted-token"]
        }
        if (key === "auth-token-bin") {
          return []
        }
        return []
      }
    }

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("metadata", mockMetadata)
        mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    const result = await client.handshake()
    expect(result).toBeDefined()
    expect(result.token).toBe("my-extracted-token")
    expect(client.getBearerToken()).toBe("my-extracted-token")
  })
})

describe("handshake error and no-response cases", () => {
  let client: FlightClient

  beforeEach(async () => {
    client = new FlightClient({ host: "localhost", port: 8815, tls: false })
    await client.connect()
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("rejects when handshake stream errors", async () => {
    const mockHandshakeStream = new EventEmitter()

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("error", new Error("connection failed"))
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    await expect(client.handshake()).rejects.toThrow("connection failed")
  })

  it("rejects when no handshake response received", async () => {
    const mockHandshakeStream = new EventEmitter()

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        // Emit "end" without any "data"
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn(),
        end: vi.fn()
      })
    })

    await expect(client.handshake()).rejects.toThrow("no handshake response received")
  })
})

describe("getFlightInfo and getSchema coverage", () => {
  let client: FlightClient

  beforeEach(async () => {
    client = new FlightClient({ host: "localhost", port: 8815, tls: false })
    await client.connect()
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("calls getFlightInfo and resolves with response", async () => {
    const mockResponse = {
      schema: Buffer.from("schema"),
      flightDescriptor: { type: 1, path: ["test"], cmd: Buffer.alloc(0) },
      endpoint: [],
      totalRecords: 100,
      totalBytes: 1000,
      ordered: false,
      appMetadata: Buffer.alloc(0)
    }

    // @ts-expect-error accessing private property
    client.grpcClient.getFlightInfo = vi.fn(
      (
        _request: unknown,
        _metadata: unknown,
        callback: (err: Error | null, res: unknown) => void
      ) => {
        callback(null, mockResponse)
      }
    )

    const result = await client.getFlightInfo({ type: "path", path: ["test"] })
    expect(result).toEqual(mockResponse)
  })

  it("calls getFlightInfo and rejects on error", async () => {
    // @ts-expect-error accessing private property
    client.grpcClient.getFlightInfo = vi.fn(
      (
        _request: unknown,
        _metadata: unknown,
        callback: (err: Error | null, res: unknown) => void
      ) => {
        callback(new Error("flight not found"), null)
      }
    )

    await expect(client.getFlightInfo({ type: "path", path: ["missing"] })).rejects.toThrow(
      "flight not found"
    )
  })

  it("calls getSchema and resolves with response", async () => {
    const mockResponse = {
      schema: Buffer.from("schema-bytes")
    }

    // @ts-expect-error accessing private property
    client.grpcClient.getSchema = vi.fn(
      (
        _request: unknown,
        _metadata: unknown,
        callback: (err: Error | null, res: unknown) => void
      ) => {
        callback(null, mockResponse)
      }
    )

    const result = await client.getSchema({ type: "cmd", cmd: Buffer.from("SELECT 1") })
    expect(result).toEqual(mockResponse)
  })

  it("calls getSchema and rejects on error", async () => {
    // @ts-expect-error accessing private property
    client.grpcClient.getSchema = vi.fn(
      (
        _request: unknown,
        _metadata: unknown,
        callback: (err: Error | null, res: unknown) => void
      ) => {
        callback(new Error("schema error"), null)
      }
    )

    await expect(client.getSchema({ type: "path", path: ["missing"] })).rejects.toThrow(
      "schema error"
    )
  })
})

describe("listFlights with criteria coverage", () => {
  let client: FlightClient
  let mockStream: EventEmitter

  beforeEach(async () => {
    mockStream = new EventEmitter()

    client = new FlightClient({ host: "localhost", port: 8815, tls: false })
    await client.connect()

    // @ts-expect-error accessing private property
    client.grpcClient.listFlights = vi.fn(() => mockStream)
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("calls listFlights with criteria parameter", async () => {
    const criteria = { expression: Buffer.from("filter") }

    setTimeout(() => {
      mockStream.emit("data", {
        schema: Buffer.alloc(0),
        flightDescriptor: { type: 1, path: ["flight1"], cmd: Buffer.alloc(0) },
        endpoint: [],
        totalRecords: 0,
        totalBytes: 0,
        ordered: false,
        appMetadata: Buffer.alloc(0)
      })
      mockStream.emit("end")
    }, 5)

    const results = []
    for await (const info of client.listFlights(criteria)) {
      results.push(info)
    }

    expect(results).toHaveLength(1)
  })

  it("calls listFlights without criteria (uses default)", async () => {
    setTimeout(() => {
      mockStream.emit("data", {
        schema: Buffer.alloc(0),
        flightDescriptor: { type: 1, path: ["flight2"], cmd: Buffer.alloc(0) },
        endpoint: [],
        totalRecords: 0,
        totalBytes: 0,
        ordered: false,
        appMetadata: Buffer.alloc(0)
      })
      mockStream.emit("end")
    }, 5)

    const results = []
    for await (const info of client.listFlights()) {
      results.push(info)
    }

    expect(results).toHaveLength(1)
  })
})

describe("doGet and streamToAsyncIterable coverage", () => {
  let client: FlightClient
  let mockStream: EventEmitter

  beforeEach(async () => {
    mockStream = new EventEmitter()

    client = new FlightClient({ host: "localhost", port: 8815, tls: false })
    await client.connect()

    // @ts-expect-error accessing private property
    client.grpcClient.doGet = vi.fn(() => mockStream)
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("iterates doGet results via streamToAsyncIterable", async () => {
    const ticket = { ticket: Buffer.from("test-ticket") }

    setTimeout(() => {
      mockStream.emit("data", {
        flightDescriptor: undefined,
        dataHeader: Buffer.from("header1"),
        dataBody: Buffer.from("body1"),
        appMetadata: Buffer.alloc(0)
      })
      mockStream.emit("data", {
        flightDescriptor: undefined,
        dataHeader: Buffer.from("header2"),
        dataBody: Buffer.from("body2"),
        appMetadata: Buffer.alloc(0)
      })
      mockStream.emit("end")
    }, 5)

    const results = []
    for await (const data of client.doGet(ticket)) {
      results.push(data)
    }

    expect(results).toHaveLength(2)
  })

  it("handles error in doGet stream", async () => {
    const ticket = { ticket: Buffer.from("test-ticket") }

    setTimeout(() => {
      mockStream.emit("error", new Error("stream failed"))
    }, 5)

    const results = []
    await expect(async () => {
      for await (const data of client.doGet(ticket)) {
        results.push(data)
      }
    }).rejects.toThrow("stream failed")
  })
})

describe("basic auth payload coverage", () => {
  beforeEach(() => {
    credentialCalls.createInsecure = 0
    credentialCalls.createSsl = 0
  })

  it("builds BasicAuth payload for basic auth type", async () => {
    const client = new FlightClient({
      host: "localhost",
      port: 8815,
      tls: false,
      auth: {
        type: "basic",
        username: "testuser",
        password: "testpass"
      }
    })

    await client.connect()

    // Mock handshake to capture the write call
    const mockHandshakeStream = new EventEmitter()
    let writtenPayload: Buffer | undefined

    // @ts-expect-error accessing private property
    client.grpcClient.handshake = vi.fn(() => {
      setTimeout(() => {
        mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
        mockHandshakeStream.emit("end")
      }, 5)
      return Object.assign(mockHandshakeStream, {
        write: vi.fn((data: { payload: Buffer }) => {
          writtenPayload = data.payload
        }),
        end: vi.fn()
      })
    })

    await client.handshake()

    // Verify payload was written and contains encoded BasicAuth
    expect(writtenPayload).toBeDefined()
    expect(writtenPayload?.length).toBeGreaterThan(0)
  })
})

describe("DoExchangeStream cancel coverage", () => {
  let client: FlightClient
  let mockStream: EventEmitter & {
    write: ReturnType<typeof vi.fn>
    end: ReturnType<typeof vi.fn>
    cancel: ReturnType<typeof vi.fn>
  }

  beforeEach(() => {
    mockStream = Object.assign(new EventEmitter(), {
      write: vi.fn(() => true),
      end: vi.fn(),
      cancel: vi.fn()
    })

    client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    const mockGrpcClient = {
      doExchange: vi.fn(() => mockStream),
      waitForReady: vi.fn((_, cb) => cb())
    }
    // @ts-expect-error accessing private property for testing
    client.grpcClient = mockGrpcClient
    // @ts-expect-error accessing private property for testing
    client._state = "connected"
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it("calls cancel on DoExchangeStream", () => {
    const exchangeStream = client.doExchange()
    exchangeStream.cancel()
    expect(mockStream.cancel).toHaveBeenCalled()
  })
})

describe("createFlightClient factory function", () => {
  it("returns connected client", async () => {
    const { createFlightClient } = await import("../../client")

    const client = await createFlightClient({
      host: "localhost",
      port: 8815,
      tls: false
    })

    expect(client).toBeInstanceOf(FlightClient)
    expect(client.isConnected).toBe(true)
  })
})
