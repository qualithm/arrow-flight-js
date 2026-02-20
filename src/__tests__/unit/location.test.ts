import { describe, expect, it } from "bun:test"

import { createLocation, LocationParseError, parseLocation } from "../../location"

describe("parseLocation", () => {
  it("parses grpc:// URI", () => {
    const result = parseLocation("grpc://localhost:8815")
    expect(result).toEqual({
      uri: "grpc://localhost:8815",
      scheme: "grpc",
      host: "localhost",
      port: 8815,
      secure: false,
      reuseConnection: false
    })
  })

  it("parses grpc+tls:// URI", () => {
    const result = parseLocation("grpc+tls://flight.example.com:443")
    expect(result).toEqual({
      uri: "grpc+tls://flight.example.com:443",
      scheme: "grpc+tls",
      host: "flight.example.com",
      port: 443,
      secure: true,
      reuseConnection: false
    })
  })

  it("parses grpc+unix:// URI", () => {
    const result = parseLocation("grpc+unix:///var/run/flight.sock")
    expect(result).toEqual({
      uri: "grpc+unix:///var/run/flight.sock",
      scheme: "grpc+unix",
      host: "/var/run/flight.sock",
      port: undefined,
      secure: false,
      reuseConnection: false
    })
  })

  it("parses http:// URI", () => {
    const result = parseLocation("http://example.com:8080")
    expect(result).toEqual({
      uri: "http://example.com:8080",
      scheme: "http",
      host: "example.com",
      port: 8080,
      secure: false,
      reuseConnection: false
    })
  })

  it("parses https:// URI", () => {
    const result = parseLocation("https://example.com")
    expect(result).toEqual({
      uri: "https://example.com",
      scheme: "https",
      host: "example.com",
      port: undefined,
      secure: true,
      reuseConnection: false
    })
  })

  it("handles empty string as reuse connection", () => {
    const result = parseLocation("")
    expect(result).toEqual({
      uri: "arrow-flight-reuse-connection://?",
      scheme: "arrow-flight-reuse-connection",
      host: "",
      secure: false,
      reuseConnection: true
    })
  })

  it("handles explicit reuse connection URI", () => {
    const result = parseLocation("arrow-flight-reuse-connection://?")
    expect(result).toEqual({
      uri: "arrow-flight-reuse-connection://?",
      scheme: "arrow-flight-reuse-connection",
      host: "",
      secure: false,
      reuseConnection: true
    })
  })

  it("accepts Location object", () => {
    const result = parseLocation({ uri: "grpc://localhost:8815" })
    expect(result.host).toBe("localhost")
    expect(result.port).toBe(8815)
  })

  it("throws LocationParseError for invalid URI", () => {
    expect(() => parseLocation("not-a-valid-uri")).toThrow(LocationParseError)
    expect(() => parseLocation("not-a-valid-uri")).toThrow("invalid uri")
  })

  it("throws LocationParseError for unsupported scheme", () => {
    expect(() => parseLocation("ftp://example.com")).toThrow(LocationParseError)
    expect(() => parseLocation("ftp://example.com")).toThrow("unsupported scheme")
  })
})

describe("createLocation", () => {
  it("creates grpc location", () => {
    const result = createLocation("grpc", "localhost", 8815)
    expect(result).toEqual({ uri: "grpc://localhost:8815" })
  })

  it("creates grpc+tls location", () => {
    const result = createLocation("grpc+tls", "flight.example.com", 443)
    expect(result).toEqual({ uri: "grpc+tls://flight.example.com:443" })
  })

  it("creates location without port", () => {
    const result = createLocation("https", "example.com")
    expect(result).toEqual({ uri: "https://example.com" })
  })

  it("creates grpc+unix location", () => {
    const result = createLocation("grpc+unix", "/var/run/flight.sock")
    expect(result).toEqual({ uri: "grpc+unix:///var/run/flight.sock" })
  })

  it("creates reuse connection location", () => {
    const result = createLocation("arrow-flight-reuse-connection", "")
    expect(result).toEqual({ uri: "arrow-flight-reuse-connection://?" })
  })
})
