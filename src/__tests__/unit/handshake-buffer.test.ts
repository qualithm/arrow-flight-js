/**
 * Test specifically for Buffer token extraction in handshake.
 * This file does NOT use vi.mock to ensure real behavior.
 */
import { Metadata } from "@grpc/grpc-js"
import { EventEmitter } from "events"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { FlightClient } from "../../client"

describe("Handshake Buffer token extraction", () => {
  let client: FlightClient
  let mockHandshakeStream: EventEmitter & {
    write: ReturnType<typeof vi.fn>
    end: ReturnType<typeof vi.fn>
  }

  beforeEach(() => {
    client = new FlightClient({ host: "localhost", port: 8815, tls: false })

    // Create a mock stream
    mockHandshakeStream = Object.assign(new EventEmitter(), {
      write: vi.fn(),
      end: vi.fn()
    })

    // Mock the grpc client with minimal setup
    const mockGrpcClient = {
      handshake: vi.fn(() => mockHandshakeStream),
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

  it("extracts token from Buffer in auth-token-bin", async () => {
    // Clear any previous token
    client.clearBearerToken()

    // Create real Metadata with a Buffer value
    const metadata = new Metadata()
    metadata.set("auth-token-bin", Buffer.from("buffer-extracted-token"))

    // Verify the metadata returns a Buffer
    const retrieved = metadata.get("auth-token-bin")
    expect(retrieved.length).toBe(1)
    expect(Buffer.isBuffer(retrieved[0])).toBe(true)

    // Trigger the handshake flow
    const handshakePromise = client.handshake()

    // Emit events asynchronously
    setTimeout(() => {
      mockHandshakeStream.emit("metadata", metadata)
      mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
      mockHandshakeStream.emit("end")
    }, 5)

    const result = await handshakePromise
    expect(result).toBeDefined()

    // The token should have been extracted from the Buffer
    expect(client.getBearerToken()).toBe("buffer-extracted-token")
  })

  it("handles non-string non-Buffer value in auth-token-bin gracefully", async () => {
    // Clear any previous token
    client.clearBearerToken()

    // Create a mock metadata that returns a number (edge case)
    const mockMetadata = {
      get: (key: string) => {
        if (key === "auth-token-bin") {
          return [12345] // Neither string nor Buffer
        }
        if (key === "authorization") {
          return []
        }
        return []
      }
    }

    // Trigger the handshake flow
    const handshakePromise = client.handshake()

    // Emit events asynchronously
    setTimeout(() => {
      mockHandshakeStream.emit("metadata", mockMetadata)
      mockHandshakeStream.emit("data", { protocolVersion: 0, payload: Buffer.alloc(0) })
      mockHandshakeStream.emit("end")
    }, 5)

    const result = await handshakePromise
    expect(result).toBeDefined()

    // No token should have been extracted (neither string nor Buffer)
    expect(client.getBearerToken()).toBeNull()
  })
})
