import { describe, expect, it } from "bun:test"

import {
  cmdDescriptor,
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
