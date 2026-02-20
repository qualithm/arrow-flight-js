/**
 * Arrow Flight protocol types.
 *
 * These types provide a clean public API surface for the Arrow Flight client,
 * wrapping the generated protobuf types where appropriate.
 *
 * @packageDocumentation
 */

import {
  type ChannelCredentials,
  type Metadata,
  status as GrpcStatus,
  type StatusObject
} from "@grpc/grpc-js"

import type {
  CancelStatus as ProtoCancelStatus,
  FlightDescriptor,
  FlightDescriptor_DescriptorType as ProtoDescriptorType
} from "./generated/arrow/flight/protocol/Flight.js"

// Re-export proto message types that are already well-designed
export type {
  Action,
  ActionType,
  BasicAuth,
  CancelFlightInfoRequest,
  CancelFlightInfoResult,
  Criteria,
  FlightData,
  FlightDescriptor,
  FlightEndpoint,
  FlightInfo,
  HandshakeRequest,
  HandshakeResponse,
  Location,
  PollInfo,
  PutResult,
  Result,
  SchemaResult,
  Ticket
} from "./generated/arrow/flight/protocol/Flight.js"

/**
 * Cancel status for flight operations.
 */
export type CancelStatus = "unspecified" | "cancelled" | "cancelling" | "not-cancellable"

/**
 * Descriptor type for identifying flights.
 */
export type DescriptorType = "path" | "cmd"

/**
 * Converts a CancelStatus to the proto enum value.
 *
 * @internal
 */
export function toCancelStatusProto(status: CancelStatus): ProtoCancelStatus {
  const mapping: Record<CancelStatus, ProtoCancelStatus> = {
    unspecified: 0,
    cancelled: 1,
    cancelling: 2,
    "not-cancellable": 3
  }
  return mapping[status]
}

/**
 * Converts a proto CancelStatus to the public type.
 *
 * @internal
 */
export function fromCancelStatusProto(status: ProtoCancelStatus): CancelStatus {
  const value = status as number
  switch (value) {
    case 1:
      return "cancelled"
    case 2:
      return "cancelling"
    case 3:
      return "not-cancellable"
    default:
      return "unspecified"
  }
}

/**
 * Converts a DescriptorType to the proto enum value.
 *
 * @internal
 */
export function toDescriptorTypeProto(type: DescriptorType): ProtoDescriptorType {
  const mapping: Record<DescriptorType, ProtoDescriptorType> = {
    path: 1,
    cmd: 2
  }
  return mapping[type]
}

/**
 * Converts a proto DescriptorType to the public type.
 *
 * @internal
 */
export function fromDescriptorTypeProto(type: ProtoDescriptorType): DescriptorType | null {
  const value = type as number
  switch (value) {
    case 1:
      return "path"
    case 2:
      return "cmd"
    default:
      return null
  }
}

/**
 * Options for creating a FlightClient connection.
 */
export type FlightClientOptions = {
  /**
   * The host to connect to.
   *
   * @example "localhost"
   * @example "flight.example.com"
   */
  host: string

  /**
   * The port to connect to.
   *
   * @default 443 for TLS, 80 for insecure
   */
  port?: number

  /**
   * Whether to use TLS for the connection.
   * Set to `false` for insecure connections.
   * Set to `true` or provide `tlsOptions` for TLS.
   *
   * @default true
   */
  tls?: boolean | TlsOptions

  /**
   * Custom channel credentials for the gRPC connection.
   * If provided, overrides the `tls` option entirely.
   *
   * Use this for advanced credential configurations not covered
   * by the `tls` and `auth` options.
   */
  credentials?: ChannelCredentials

  /**
   * Additional gRPC channel options.
   */
  channelOptions?: FlightChannelOptions

  /**
   * Authentication configuration.
   */
  auth?: FlightAuthOptions
}

/**
 * TLS configuration options for secure connections.
 */
export type TlsOptions = {
  /**
   * Root CA certificate(s) in PEM format for server verification.
   * If not provided, the system's default CA bundle is used.
   *
   * @example
   * ```ts
   * { rootCerts: fs.readFileSync('/path/to/ca.crt') }
   * ```
   */
  rootCerts?: Buffer | string

  /**
   * Client certificate chain in PEM format for mTLS.
   * Must be provided together with `privateKey`.
   */
  certChain?: Buffer | string

  /**
   * Client private key in PEM format for mTLS.
   * Must be provided together with `certChain`.
   */
  privateKey?: Buffer | string

  /**
   * Override the SSL target name for certificate verification.
   * Useful when connecting via IP address or when the server certificate
   * has a different hostname than the connection target.
   *
   * @example "flight.example.com"
   */
  serverNameOverride?: string

  /**
   * Whether to verify the server certificate.
   * Set to `false` to skip verification (NOT RECOMMENDED for production).
   *
   * @default true
   */
  verifyServerCert?: boolean
}

/**
 * gRPC channel options for fine-tuning connection behaviour.
 */
export type FlightChannelOptions = {
  /**
   * Maximum message size the client can receive (in bytes).
   *
   * @default 4 * 1024 * 1024 (4MB)
   */
  maxReceiveMessageLength?: number

  /**
   * Maximum message size the client can send (in bytes).
   *
   * @default 4 * 1024 * 1024 (4MB)
   */
  maxSendMessageLength?: number

  /**
   * Keepalive time in milliseconds.
   * Time between keepalive pings.
   */
  keepaliveTimeMs?: number

  /**
   * Keepalive timeout in milliseconds.
   * Time to wait for a keepalive ping response.
   */
  keepaliveTimeoutMs?: number

  /**
   * Whether to allow keepalive pings when there are no active calls.
   */
  keepalivePermitWithoutCalls?: boolean

  /**
   * Initial connection timeout in milliseconds.
   */
  connectTimeoutMs?: number
}

/**
 * Authentication options for the Flight client.
 */
export type FlightAuthOptions =
  | FlightBasicAuthOptions
  | FlightBearerTokenOptions
  | FlightMTLSOptions
  | FlightHandshakeOptions

/**
 * Basic username/password authentication.
 */
export type FlightBasicAuthOptions = {
  type: "basic"
  username: string
  password: string
}

/**
 * Bearer token authentication.
 */
export type FlightBearerTokenOptions = {
  type: "bearer"
  token: string
}

/**
 * Mutual TLS (mTLS) authentication using client certificates.
 */
export type FlightMTLSOptions = {
  type: "mtls"
  /**
   * Client certificate in PEM format.
   */
  cert: Buffer | string
  /**
   * Client private key in PEM format.
   */
  key: Buffer | string
  /**
   * CA certificate(s) for server verification in PEM format.
   * If not provided, the system's default CA bundle is used.
   */
  ca?: Buffer | string
}

/**
 * Flight Handshake authentication.
 * Uses the Flight-specific handshake mechanism for authentication.
 */
export type FlightHandshakeOptions = {
  type: "handshake"
  /**
   * Custom handshake payload.
   */
  payload: Buffer
}

/**
 * Call-level options that can be passed to individual RPC methods.
 */
export type CallOptions = {
  /**
   * Timeout for this call in milliseconds.
   */
  timeoutMs?: number

  /**
   * Additional metadata headers to send with this call.
   */
  headers?: Record<string, string>
}

/**
 * Creates a path-based flight descriptor.
 *
 * @param path - The path segments identifying the dataset
 * @returns A FlightDescriptor for the given path
 *
 * @example
 * ```ts
 * const descriptor = pathDescriptor("database", "schema", "table")
 * ```
 */
export function pathDescriptor(...path: string[]): PathDescriptor {
  return {
    type: "path" as const,
    path
  }
}

/**
 * Creates a command-based flight descriptor.
 *
 * @param cmd - The command bytes
 * @returns A FlightDescriptor for the given command
 *
 * @example
 * ```ts
 * const descriptor = cmdDescriptor(Buffer.from("SELECT * FROM table"))
 * ```
 */
export function cmdDescriptor(cmd: Buffer): CmdDescriptor {
  return {
    type: "cmd" as const,
    cmd
  }
}

/**
 * A path-based descriptor identifying a dataset by its path.
 */
export type PathDescriptor = {
  type: "path"
  path: string[]
}

/**
 * A command-based descriptor identifying a dataset by an opaque command.
 */
export type CmdDescriptor = {
  type: "cmd"
  cmd: Buffer
}

/**
 * Union type for descriptors that can be passed to Flight methods.
 */
export type Descriptor = PathDescriptor | CmdDescriptor

/**
 * Converts a user-friendly Descriptor to a FlightDescriptor proto message.
 *
 * @internal
 */
export function toFlightDescriptor(descriptor: Descriptor): FlightDescriptor {
  if (descriptor.type === "path") {
    return {
      type: toDescriptorTypeProto("path"),
      path: descriptor.path,
      cmd: Buffer.alloc(0)
    }
  }

  return {
    type: toDescriptorTypeProto("cmd"),
    cmd: descriptor.cmd,
    path: []
  }
}

/**
 * Result from a DoGet operation, providing access to Arrow data.
 */
export type DoGetResult = {
  /**
   * The Arrow schema for the data stream.
   */
  schema: Buffer

  /**
   * Async iterator over the data batches.
   */
  batches: AsyncIterable<Buffer>
}

/**
 * Result from a DoPut operation.
 */
export type DoPutResult = {
  /**
   * Application-specific metadata returned from the server.
   */
  appMetadata?: Buffer
}

/**
 * Errors that can be thrown by Flight operations.
 *
 * FlightError wraps gRPC errors with a typed error code and optional
 * additional metadata. Use the static helper methods or `isFlightError()`
 * to check error types.
 *
 * @example
 * ```ts
 * try {
 *   await client.getFlightInfo(descriptor)
 * } catch (error) {
 *   if (FlightError.isNotFound(error)) {
 *     console.log("Flight not found")
 *   } else if (FlightError.isUnauthenticated(error)) {
 *     console.log("Authentication required")
 *   }
 * }
 * ```
 */
export class FlightError extends Error {
  readonly code: FlightErrorCode
  readonly details?: string
  readonly metadata?: Record<string, string>
  readonly grpcCode?: number

  constructor(
    message: string,
    code: FlightErrorCode,
    options?: {
      details?: string
      metadata?: Record<string, string>
      grpcCode?: number
      cause?: Error
    }
  ) {
    super(message, options?.cause !== undefined ? { cause: options.cause } : undefined)
    this.name = "FlightError"
    this.code = code
    this.details = options?.details
    this.metadata = options?.metadata
    this.grpcCode = options?.grpcCode
  }

  /**
   * Creates a FlightError from a gRPC error.
   *
   * @internal
   */
  static fromGrpcError(error: unknown): FlightError {
    if (error instanceof FlightError) {
      return error
    }

    // Check if it's a gRPC ServiceError (has code property)
    if (isGrpcServiceError(error)) {
      const code = grpcStatusToFlightCode(error.code)
      const metadata = extractGrpcMetadata(error.metadata)

      const message = error.details.length > 0 ? error.details : error.message
      return new FlightError(message, code, {
        details: error.details.length > 0 ? error.details : undefined,
        metadata,
        grpcCode: error.code,
        cause: error
      })
    }

    if (error instanceof Error) {
      return new FlightError(error.message, "UNKNOWN", {
        cause: error
      })
    }

    return new FlightError(String(error), "UNKNOWN")
  }

  /**
   * Checks if an error is a FlightError.
   */
  static isFlightError(error: unknown): error is FlightError {
    return error instanceof FlightError
  }

  /**
   * Checks if an error indicates the resource was not found.
   */
  static isNotFound(error: unknown): boolean {
    return error instanceof FlightError && error.code === "NOT_FOUND"
  }

  /**
   * Checks if an error indicates authentication is required.
   */
  static isUnauthenticated(error: unknown): boolean {
    return error instanceof FlightError && error.code === "UNAUTHENTICATED"
  }

  /**
   * Checks if an error indicates permission was denied.
   */
  static isPermissionDenied(error: unknown): boolean {
    return error instanceof FlightError && error.code === "PERMISSION_DENIED"
  }

  /**
   * Checks if an error indicates an invalid argument was provided.
   */
  static isInvalidArgument(error: unknown): boolean {
    return error instanceof FlightError && error.code === "INVALID_ARGUMENT"
  }

  /**
   * Checks if an error indicates the operation was cancelled.
   */
  static isCancelled(error: unknown): boolean {
    return error instanceof FlightError && error.code === "CANCELLED"
  }

  /**
   * Checks if an error indicates the deadline was exceeded.
   */
  static isDeadlineExceeded(error: unknown): boolean {
    return error instanceof FlightError && error.code === "DEADLINE_EXCEEDED"
  }

  /**
   * Checks if an error indicates the service is unavailable.
   */
  static isUnavailable(error: unknown): boolean {
    return error instanceof FlightError && error.code === "UNAVAILABLE"
  }

  /**
   * Checks if an error indicates the operation is not implemented.
   */
  static isUnimplemented(error: unknown): boolean {
    return error instanceof FlightError && error.code === "UNIMPLEMENTED"
  }

  /**
   * Checks if the error is retriable (transient failure).
   */
  static isRetriable(error: unknown): boolean {
    if (!(error instanceof FlightError)) {
      return false
    }
    return (
      error.code === "UNAVAILABLE" ||
      error.code === "RESOURCE_EXHAUSTED" ||
      error.code === "ABORTED"
    )
  }

  /**
   * Returns a string representation of the error.
   */
  override toString(): string {
    let result = `FlightError [${this.code}]: ${this.message}`
    if (this.details !== undefined && this.details !== this.message) {
      result += ` (${this.details})`
    }
    return result
  }
}

/**
 * Type guard to check if an error is a gRPC ServiceError.
 *
 * @internal
 */
function isGrpcServiceError(error: unknown): error is GrpcServiceError {
  return (
    error !== null &&
    typeof error === "object" &&
    "code" in error &&
    typeof (error as GrpcServiceError).code === "number" &&
    "message" in error
  )
}

/**
 * gRPC ServiceError shape for type checking.
 *
 * @internal
 */
type GrpcServiceError = StatusObject & Error

/**
 * Converts a gRPC status code to a FlightErrorCode.
 *
 * @internal
 */
function grpcStatusToFlightCode(code: number): FlightErrorCode {
  const mapping: Record<number, FlightErrorCode> = {
    [GrpcStatus.CANCELLED]: "CANCELLED",
    [GrpcStatus.UNKNOWN]: "UNKNOWN",
    [GrpcStatus.INVALID_ARGUMENT]: "INVALID_ARGUMENT",
    [GrpcStatus.DEADLINE_EXCEEDED]: "DEADLINE_EXCEEDED",
    [GrpcStatus.NOT_FOUND]: "NOT_FOUND",
    [GrpcStatus.ALREADY_EXISTS]: "ALREADY_EXISTS",
    [GrpcStatus.PERMISSION_DENIED]: "PERMISSION_DENIED",
    [GrpcStatus.RESOURCE_EXHAUSTED]: "RESOURCE_EXHAUSTED",
    [GrpcStatus.FAILED_PRECONDITION]: "FAILED_PRECONDITION",
    [GrpcStatus.ABORTED]: "ABORTED",
    [GrpcStatus.OUT_OF_RANGE]: "OUT_OF_RANGE",
    [GrpcStatus.UNIMPLEMENTED]: "UNIMPLEMENTED",
    [GrpcStatus.INTERNAL]: "INTERNAL",
    [GrpcStatus.UNAVAILABLE]: "UNAVAILABLE",
    [GrpcStatus.DATA_LOSS]: "DATA_LOSS",
    [GrpcStatus.UNAUTHENTICATED]: "UNAUTHENTICATED"
  }
  return mapping[code] ?? "UNKNOWN"
}

/**
 * Extracts metadata from a gRPC error as a plain object.
 *
 * @internal
 */
function extractGrpcMetadata(metadata: Metadata | undefined): Record<string, string> | undefined {
  if (metadata === undefined) {
    return undefined
  }

  const result: Record<string, string> = {}
  const map = metadata.getMap()
  let hasEntries = false

  for (const [key, value] of Object.entries(map)) {
    if (typeof value === "string") {
      result[key] = value
      hasEntries = true
    }
  }

  return hasEntries ? result : undefined
}

/**
 * gRPC status codes that may be returned by Flight operations.
 */
export type FlightErrorCode =
  | "CANCELLED"
  | "UNKNOWN"
  | "INVALID_ARGUMENT"
  | "DEADLINE_EXCEEDED"
  | "NOT_FOUND"
  | "ALREADY_EXISTS"
  | "PERMISSION_DENIED"
  | "RESOURCE_EXHAUSTED"
  | "FAILED_PRECONDITION"
  | "ABORTED"
  | "OUT_OF_RANGE"
  | "UNIMPLEMENTED"
  | "INTERNAL"
  | "UNAVAILABLE"
  | "DATA_LOSS"
  | "UNAUTHENTICATED"
