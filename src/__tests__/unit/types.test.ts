import { Metadata } from "@grpc/grpc-js"
import { describe, expect, it } from "bun:test"

import {
  cmdDescriptor,
  FlightError,
  type FlightErrorCode,
  fromCancelStatusProto,
  fromDescriptorTypeProto,
  pathDescriptor,
  toCancelStatusProto,
  toDescriptorTypeProto,
  toFlightDescriptor
} from "../../types"

describe("pathDescriptor", () => {
  it("creates a path descriptor with single segment", () => {
    const result = pathDescriptor("table")
    expect(result).toEqual({ type: "path", path: ["table"] })
  })

  it("creates a path descriptor with multiple segments", () => {
    const result = pathDescriptor("database", "schema", "table")
    expect(result).toEqual({ type: "path", path: ["database", "schema", "table"] })
  })

  it("creates an empty path descriptor", () => {
    const result = pathDescriptor()
    expect(result).toEqual({ type: "path", path: [] })
  })
})

describe("cmdDescriptor", () => {
  it("creates a command descriptor", () => {
    const cmd = Buffer.from("SELECT * FROM users")
    const result = cmdDescriptor(cmd)
    expect(result).toEqual({ type: "cmd", cmd })
  })

  it("handles empty buffer", () => {
    const cmd = Buffer.alloc(0)
    const result = cmdDescriptor(cmd)
    expect(result).toEqual({ type: "cmd", cmd })
  })
})

describe("toFlightDescriptor", () => {
  it("converts path descriptor to proto format", () => {
    const descriptor = pathDescriptor("database", "schema", "table")
    const result = toFlightDescriptor(descriptor)

    expect(result.type).toBe(1) // PATH
    expect(result.path).toEqual(["database", "schema", "table"])
    expect(result.cmd).toEqual(Buffer.alloc(0))
  })

  it("converts command descriptor to proto format", () => {
    const cmd = Buffer.from("SELECT 1")
    const descriptor = cmdDescriptor(cmd)
    const result = toFlightDescriptor(descriptor)

    expect(result.type).toBe(2) // CMD
    expect(result.cmd).toEqual(cmd)
    expect(result.path).toEqual([])
  })
})

describe("CancelStatus conversion", () => {
  it("converts to proto values", () => {
    expect(toCancelStatusProto("unspecified")).toBe(0)
    expect(toCancelStatusProto("cancelled")).toBe(1)
    expect(toCancelStatusProto("cancelling")).toBe(2)
    expect(toCancelStatusProto("not-cancellable")).toBe(3)
  })

  it("converts from proto values", () => {
    expect(fromCancelStatusProto(0)).toBe("unspecified")
    expect(fromCancelStatusProto(1)).toBe("cancelled")
    expect(fromCancelStatusProto(2)).toBe("cancelling")
    expect(fromCancelStatusProto(3)).toBe("not-cancellable")
  })

  it("handles unknown proto values", () => {
    expect(fromCancelStatusProto(-1)).toBe("unspecified")
    expect(fromCancelStatusProto(99 as never)).toBe("unspecified")
  })
})

describe("DescriptorType conversion", () => {
  it("converts to proto values", () => {
    expect(toDescriptorTypeProto("path")).toBe(1)
    expect(toDescriptorTypeProto("cmd")).toBe(2)
  })

  it("converts from proto values", () => {
    expect(fromDescriptorTypeProto(1)).toBe("path")
    expect(fromDescriptorTypeProto(2)).toBe("cmd")
  })

  it("returns null for unknown values", () => {
    expect(fromDescriptorTypeProto(0)).toBeNull() // UNKNOWN
    expect(fromDescriptorTypeProto(-1)).toBeNull() // UNRECOGNIZED
    expect(fromDescriptorTypeProto(99 as never)).toBeNull()
  })
})

describe("FlightError", () => {
  describe("constructor", () => {
    it("creates error with basic properties", () => {
      const error = new FlightError("test message", "NOT_FOUND")
      expect(error.message).toBe("test message")
      expect(error.code).toBe("NOT_FOUND")
      expect(error.name).toBe("FlightError")
    })

    it("creates error with optional details", () => {
      const error = new FlightError("test message", "INVALID_ARGUMENT", {
        details: "additional info"
      })
      expect(error.details).toBe("additional info")
    })

    it("creates error with metadata", () => {
      const error = new FlightError("test message", "INTERNAL", {
        metadata: { "x-custom": "value" }
      })
      expect(error.metadata).toEqual({ "x-custom": "value" })
    })

    it("creates error with grpcCode", () => {
      const error = new FlightError("test message", "NOT_FOUND", {
        grpcCode: 5
      })
      expect(error.grpcCode).toBe(5)
    })
  })

  describe("fromGrpcError", () => {
    it("returns FlightError unchanged", () => {
      const original = new FlightError("original", "CANCELLED")
      const result = FlightError.fromGrpcError(original)
      expect(result).toBe(original)
    })

    it("converts gRPC ServiceError with code", () => {
      const grpcError = Object.assign(new Error("not found"), {
        code: 5, // NOT_FOUND
        details: "resource not found",
        metadata: new Metadata()
      })
      const result = FlightError.fromGrpcError(grpcError)

      expect(result.message).toBe("resource not found")
      expect(result.code).toBe("NOT_FOUND")
      expect(result.grpcCode).toBe(5)
    })

    it("converts gRPC errors with metadata", () => {
      const metadata = new Metadata()
      metadata.set("x-request-id", "abc123")

      const grpcError = Object.assign(new Error("error"), {
        code: 13, // INTERNAL
        details: "internal error",
        metadata
      })
      const result = FlightError.fromGrpcError(grpcError)

      expect(result.metadata).toEqual({ "x-request-id": "abc123" })
    })

    it("converts regular Error to UNKNOWN", () => {
      const error = new Error("something went wrong")
      const result = FlightError.fromGrpcError(error)

      expect(result.code).toBe("UNKNOWN")
      expect(result.message).toBe("something went wrong")
    })

    it("converts string to UNKNOWN", () => {
      const result = FlightError.fromGrpcError("string error")
      expect(result.code).toBe("UNKNOWN")
      expect(result.message).toBe("string error")
    })

    it("maps all gRPC status codes correctly", () => {
      const mappings: [number, FlightErrorCode][] = [
        [1, "CANCELLED"],
        [2, "UNKNOWN"],
        [3, "INVALID_ARGUMENT"],
        [4, "DEADLINE_EXCEEDED"],
        [5, "NOT_FOUND"],
        [6, "ALREADY_EXISTS"],
        [7, "PERMISSION_DENIED"],
        [8, "RESOURCE_EXHAUSTED"],
        [9, "FAILED_PRECONDITION"],
        [10, "ABORTED"],
        [11, "OUT_OF_RANGE"],
        [12, "UNIMPLEMENTED"],
        [13, "INTERNAL"],
        [14, "UNAVAILABLE"],
        [15, "DATA_LOSS"],
        [16, "UNAUTHENTICATED"]
      ]

      for (const [grpcCode, expectedCode] of mappings) {
        const grpcError = Object.assign(new Error("test"), {
          code: grpcCode,
          details: "test",
          metadata: new Metadata()
        })
        const result = FlightError.fromGrpcError(grpcError)
        expect(result.code).toBe(expectedCode)
      }
    })
  })

  describe("type checking helpers", () => {
    it("isFlightError returns true for FlightError", () => {
      const error = new FlightError("test", "UNKNOWN")
      expect(FlightError.isFlightError(error)).toBe(true)
    })

    it("isFlightError returns false for other errors", () => {
      expect(FlightError.isFlightError(new Error("test"))).toBe(false)
      expect(FlightError.isFlightError("string")).toBe(false)
      expect(FlightError.isFlightError(null)).toBe(false)
    })

    it("isNotFound checks NOT_FOUND code", () => {
      expect(FlightError.isNotFound(new FlightError("test", "NOT_FOUND"))).toBe(true)
      expect(FlightError.isNotFound(new FlightError("test", "UNKNOWN"))).toBe(false)
      expect(FlightError.isNotFound(new Error("test"))).toBe(false)
    })

    it("isUnauthenticated checks UNAUTHENTICATED code", () => {
      expect(FlightError.isUnauthenticated(new FlightError("test", "UNAUTHENTICATED"))).toBe(true)
      expect(FlightError.isUnauthenticated(new FlightError("test", "UNKNOWN"))).toBe(false)
    })

    it("isPermissionDenied checks PERMISSION_DENIED code", () => {
      expect(FlightError.isPermissionDenied(new FlightError("test", "PERMISSION_DENIED"))).toBe(
        true
      )
      expect(FlightError.isPermissionDenied(new FlightError("test", "UNKNOWN"))).toBe(false)
    })

    it("isInvalidArgument checks INVALID_ARGUMENT code", () => {
      expect(FlightError.isInvalidArgument(new FlightError("test", "INVALID_ARGUMENT"))).toBe(true)
      expect(FlightError.isInvalidArgument(new FlightError("test", "UNKNOWN"))).toBe(false)
    })

    it("isCancelled checks CANCELLED code", () => {
      expect(FlightError.isCancelled(new FlightError("test", "CANCELLED"))).toBe(true)
      expect(FlightError.isCancelled(new FlightError("test", "UNKNOWN"))).toBe(false)
    })

    it("isDeadlineExceeded checks DEADLINE_EXCEEDED code", () => {
      expect(FlightError.isDeadlineExceeded(new FlightError("test", "DEADLINE_EXCEEDED"))).toBe(
        true
      )
      expect(FlightError.isDeadlineExceeded(new FlightError("test", "UNKNOWN"))).toBe(false)
    })

    it("isUnavailable checks UNAVAILABLE code", () => {
      expect(FlightError.isUnavailable(new FlightError("test", "UNAVAILABLE"))).toBe(true)
      expect(FlightError.isUnavailable(new FlightError("test", "UNKNOWN"))).toBe(false)
    })

    it("isUnimplemented checks UNIMPLEMENTED code", () => {
      expect(FlightError.isUnimplemented(new FlightError("test", "UNIMPLEMENTED"))).toBe(true)
      expect(FlightError.isUnimplemented(new FlightError("test", "UNKNOWN"))).toBe(false)
    })
  })

  describe("isRetriable", () => {
    it("returns true for retriable errors", () => {
      expect(FlightError.isRetriable(new FlightError("test", "UNAVAILABLE"))).toBe(true)
      expect(FlightError.isRetriable(new FlightError("test", "RESOURCE_EXHAUSTED"))).toBe(true)
      expect(FlightError.isRetriable(new FlightError("test", "ABORTED"))).toBe(true)
    })

    it("returns false for non-retriable errors", () => {
      expect(FlightError.isRetriable(new FlightError("test", "NOT_FOUND"))).toBe(false)
      expect(FlightError.isRetriable(new FlightError("test", "INVALID_ARGUMENT"))).toBe(false)
      expect(FlightError.isRetriable(new FlightError("test", "PERMISSION_DENIED"))).toBe(false)
    })

    it("returns false for non-FlightError", () => {
      expect(FlightError.isRetriable(new Error("test"))).toBe(false)
    })
  })

  describe("toString", () => {
    it("formats error without details", () => {
      const error = new FlightError("something failed", "INTERNAL")
      expect(error.toString()).toBe("FlightError [INTERNAL]: something failed")
    })

    it("formats error with different details", () => {
      const error = new FlightError("something failed", "INTERNAL", {
        details: "extra info"
      })
      expect(error.toString()).toBe("FlightError [INTERNAL]: something failed (extra info)")
    })

    it("does not duplicate message in details", () => {
      const error = new FlightError("same message", "INTERNAL", {
        details: "same message"
      })
      expect(error.toString()).toBe("FlightError [INTERNAL]: same message")
    })
  })
})
