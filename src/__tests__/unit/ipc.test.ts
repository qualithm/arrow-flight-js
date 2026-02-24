import { describe, expect, it } from "vitest"

import type { FlightData } from "../../generated/arrow/flight/protocol/Flight.js"
import { collectFlightDataAsIpc, flightDataToIpc } from "../../ipc"

/**
 * Creates a mock FlightData object for testing.
 * Uses type assertion since Uint8Array is compatible with Buffer at runtime.
 */
function createFlightData(
  dataHeader: Uint8Array,
  dataBody: Uint8Array = new Uint8Array(0)
): FlightData {
  return {
    flightDescriptor: undefined,
    dataHeader: dataHeader as unknown as Buffer,
    dataBody: dataBody as unknown as Buffer,
    appMetadata: new Uint8Array(0) as unknown as Buffer
  }
}

describe("flightDataToIpc", () => {
  describe("empty input", () => {
    it("returns empty Uint8Array for empty array", () => {
      const result = flightDataToIpc([])
      expect(result).toBeInstanceOf(Uint8Array)
      expect(result.length).toBe(0)
    })

    it("skips FlightData with empty dataHeader", () => {
      const flightData: FlightData[] = [
        createFlightData(new Uint8Array(0), new Uint8Array([1, 2, 3]))
      ]
      const result = flightDataToIpc(flightData)
      expect(result.length).toBe(0)
    })
  })

  describe("single message", () => {
    it("frames a simple message correctly", () => {
      // Create a simple header (8 bytes, already aligned)
      const header = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])
      const flightData = [createFlightData(header)]

      const result = flightDataToIpc(flightData)

      // Should be: 4 (continuation) + 4 (length) + 8 (header) + 0 (padding) = 16 bytes
      expect(result.length).toBe(16)

      const view = new DataView(result.buffer)

      // Check continuation token (0xFFFFFFFF in little-endian)
      expect(view.getUint32(0, true)).toBe(0xffffffff)

      // Check metadata length
      expect(view.getUint32(4, true)).toBe(8)

      // Check header bytes
      expect(result.slice(8, 16)).toEqual(header)
    })

    it("pads header to 8-byte boundary", () => {
      // Create a header that needs padding (5 bytes)
      const header = new Uint8Array([1, 2, 3, 4, 5])
      const flightData = [createFlightData(header)]

      const result = flightDataToIpc(flightData)

      // Should be: 4 (continuation) + 4 (length) + 5 (header) + 3 (padding) = 16 bytes
      expect(result.length).toBe(16)

      const view = new DataView(result.buffer)

      // Check metadata length (original size, not padded)
      expect(view.getUint32(4, true)).toBe(5)

      // Check header bytes
      expect(result.slice(8, 13)).toEqual(header)

      // Check padding is zeros
      expect(result.slice(13, 16)).toEqual(new Uint8Array([0, 0, 0]))
    })

    it("includes data body after header", () => {
      const header = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])
      const body = new Uint8Array([10, 20, 30, 40])
      const flightData = [createFlightData(header, body)]

      const result = flightDataToIpc(flightData)

      // Should be: 4 + 4 + 8 + 0 (padding) + 4 (body) = 20 bytes
      expect(result.length).toBe(20)

      // Check body bytes at the end
      expect(result.slice(16, 20)).toEqual(body)
    })
  })

  describe("multiple messages", () => {
    it("concatenates multiple messages correctly", () => {
      const header1 = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])
      const header2 = new Uint8Array([11, 12, 13, 14, 15, 16, 17, 18])
      const flightData = [createFlightData(header1), createFlightData(header2)]

      const result = flightDataToIpc(flightData)

      // Each message: 4 + 4 + 8 = 16 bytes, so total = 32
      expect(result.length).toBe(32)

      const view = new DataView(result.buffer)

      // First message continuation token
      expect(view.getUint32(0, true)).toBe(0xffffffff)

      // Second message continuation token
      expect(view.getUint32(16, true)).toBe(0xffffffff)
    })

    it("handles mixed message sizes", () => {
      const header1 = new Uint8Array([1, 2, 3]) // 3 bytes, needs 5 padding
      const body1 = new Uint8Array([10, 20]) // 2 bytes
      const header2 = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) // 10 bytes, needs 6 padding

      const flightData = [createFlightData(header1, body1), createFlightData(header2)]

      const result = flightDataToIpc(flightData)

      // Message 1: 4 + 4 + 3 + 5 + 2 = 18 bytes
      // Message 2: 4 + 4 + 10 + 6 = 24 bytes
      // Total: 42 bytes
      expect(result.length).toBe(42)
    })
  })

  describe("padding calculation", () => {
    it("calculates correct padding for various header lengths", () => {
      const testCases = [
        { headerLen: 1, expectedPadding: 7, total: 4 + 4 + 1 + 7 },
        { headerLen: 2, expectedPadding: 6, total: 4 + 4 + 2 + 6 },
        { headerLen: 3, expectedPadding: 5, total: 4 + 4 + 3 + 5 },
        { headerLen: 4, expectedPadding: 4, total: 4 + 4 + 4 + 4 },
        { headerLen: 5, expectedPadding: 3, total: 4 + 4 + 5 + 3 },
        { headerLen: 6, expectedPadding: 2, total: 4 + 4 + 6 + 2 },
        { headerLen: 7, expectedPadding: 1, total: 4 + 4 + 7 + 1 },
        { headerLen: 8, expectedPadding: 0, total: 4 + 4 + 8 + 0 },
        { headerLen: 9, expectedPadding: 7, total: 4 + 4 + 9 + 7 },
        { headerLen: 16, expectedPadding: 0, total: 4 + 4 + 16 + 0 }
      ]

      for (const { headerLen, total } of testCases) {
        const header = new Uint8Array(headerLen).fill(1)
        const result = flightDataToIpc([createFlightData(header)])
        expect(result.length).toBe(total)
      }
    })
  })
})

describe("collectFlightDataAsIpc", () => {
  it("collects from async iterable and converts to IPC", async () => {
    const header1 = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])
    const header2 = new Uint8Array([9, 10, 11, 12, 13, 14, 15, 16])

    async function* mockStream(): AsyncGenerator<FlightData> {
      yield await Promise.resolve(createFlightData(header1))
      yield createFlightData(header2)
    }

    const result = await collectFlightDataAsIpc(mockStream())

    // Same as flightDataToIpc with two 8-byte headers
    expect(result.length).toBe(32)
  })

  it("returns empty array for empty stream", async () => {
    // Create an empty async iterable without a generator
    const emptyStream: AsyncIterable<FlightData> = {
      [Symbol.asyncIterator]: () => ({
        // eslint-disable-next-line @typescript-eslint/promise-function-async
        next: () => Promise.resolve({ done: true as const, value: undefined })
      })
    }

    const result = await collectFlightDataAsIpc(emptyStream)
    expect(result.length).toBe(0)
  })

  it("handles stream with single message", async () => {
    const header = new Uint8Array([1, 2, 3, 4])

    async function* singleStream(): AsyncGenerator<FlightData> {
      yield await Promise.resolve(createFlightData(header))
    }

    const result = await collectFlightDataAsIpc(singleStream())

    // 4 + 4 + 4 + 4 (padding) = 16
    expect(result.length).toBe(16)
  })
})
