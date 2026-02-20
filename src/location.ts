import type { Location } from "./types.js"

/**
 * Supported Flight location URI schemes.
 */
export type LocationScheme =
  | "grpc"
  | "grpc+tls"
  | "grpc+unix"
  | "http"
  | "https"
  | "arrow-flight-reuse-connection"

/**
 * Parsed location details from a Flight URI.
 */
export type ParsedLocation = {
  /** Original URI string. */
  uri: string
  /** URI scheme (e.g., "grpc", "grpc+tls"). */
  scheme: LocationScheme
  /** Hostname or Unix socket path. */
  host: string
  /** Port number, if specified. */
  port?: number
  /** Whether TLS/SSL is enabled. */
  secure: boolean
  /** Whether this indicates connection reuse. */
  reuseConnection: boolean
}

/**
 * Error thrown when a location URI is invalid or unsupported.
 */
export class LocationParseError extends Error {
  override name = "LocationParseError" as const
}

const VALID_SCHEMES = new Set<LocationScheme>([
  "grpc",
  "grpc+tls",
  "grpc+unix",
  "http",
  "https",
  "arrow-flight-reuse-connection"
])

const REUSE_CONNECTION_URI = "arrow-flight-reuse-connection://?"

/**
 * Parse a Flight location URI into its components.
 *
 * Supports the following URI schemes:
 * - `grpc://` - Plain gRPC connection
 * - `grpc+tls://` - gRPC with TLS
 * - `grpc+unix://` - gRPC over Unix socket
 * - `http://` - HTTP connection
 * - `https://` - HTTPS connection
 * - `arrow-flight-reuse-connection://?` - Reuse existing connection
 *
 * @param location - The location to parse (string or Location object).
 * @returns Parsed location details.
 * @throws {LocationParseError} If the URI is invalid or uses an unsupported scheme.
 *
 * @example
 * ```ts
 * const location = parseLocation("grpc+tls://flight.example.com:443")
 * // => {
 * //   uri: "grpc+tls://flight.example.com:443",
 * //   scheme: "grpc+tls",
 * //   host: "flight.example.com",
 * //   port: 443,
 * //   secure: true,
 * //   reuseConnection: false
 * // }
 *
 * const reuse = parseLocation("")
 * // => { ..., reuseConnection: true }
 * ```
 */
export function parseLocation(location: Location | string): ParsedLocation {
  const uri = typeof location === "string" ? location : location.uri

  // Empty string or reuse URI means reuse current connection
  if (uri === "" || uri === REUSE_CONNECTION_URI) {
    return {
      uri: uri || REUSE_CONNECTION_URI,
      scheme: "arrow-flight-reuse-connection",
      host: "",
      secure: false,
      reuseConnection: true
    }
  }

  // Parse the URI
  let parsed: URL
  try {
    parsed = new URL(uri)
  } catch {
    throw new LocationParseError(`invalid uri: ${uri}`)
  }

  // Extract and validate scheme (remove trailing colon)
  const schemeRaw = parsed.protocol.slice(0, -1)
  if (!VALID_SCHEMES.has(schemeRaw as LocationScheme)) {
    throw new LocationParseError(`unsupported scheme: ${schemeRaw}`)
  }
  const scheme = schemeRaw as LocationScheme

  // Determine host - for Unix sockets use pathname
  const host = scheme === "grpc+unix" ? parsed.pathname : parsed.hostname

  // Parse port if present
  const port = parsed.port ? Number.parseInt(parsed.port, 10) : undefined

  // Determine if connection is secure
  const secure = scheme === "grpc+tls" || scheme === "https"

  return {
    uri,
    scheme,
    host,
    port,
    secure,
    reuseConnection: false
  }
}

/**
 * Create a Location object from components.
 *
 * @param scheme - The URI scheme to use.
 * @param host - Hostname or socket path.
 * @param port - Optional port number.
 * @returns A Location object with the constructed URI.
 *
 * @example
 * ```ts
 * const location = createLocation("grpc+tls", "flight.example.com", 443)
 * // => { uri: "grpc+tls://flight.example.com:443" }
 * ```
 */
export function createLocation(scheme: LocationScheme, host: string, port?: number): Location {
  if (scheme === "arrow-flight-reuse-connection") {
    return { uri: REUSE_CONNECTION_URI }
  }

  const portSuffix = port !== undefined ? `:${String(port)}` : ""
  const uri = scheme === "grpc+unix" ? `${scheme}://${host}` : `${scheme}://${host}${portSuffix}`

  return { uri }
}
