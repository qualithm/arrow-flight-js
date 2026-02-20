/**
 * Arrow Flight client for JavaScript and TypeScript.
 *
 * Provides a high-performance transport layer for Apache Arrow data streams using gRPC.
 *
 * @packageDocumentation
 */

// Core types
export type {
  // Proto types (re-exported)
  Action,
  ActionType,
  BasicAuth,
  CallOptions,
  CancelFlightInfoRequest,
  CancelFlightInfoResult,
  // Status types
  CancelStatus,
  CmdDescriptor,
  Criteria,
  Descriptor,
  DescriptorType,
  // Results
  DoGetResult,
  DoPutResult,
  FlightAuthOptions,
  FlightBasicAuthOptions,
  FlightBearerTokenOptions,
  FlightChannelOptions,
  // Client options
  FlightClientOptions,
  FlightData,
  FlightDescriptor,
  FlightEndpoint,
  // Errors
  FlightErrorCode,
  FlightHandshakeOptions,
  FlightInfo,
  FlightMTLSOptions,
  HandshakeRequest,
  HandshakeResponse,
  Location,
  // Descriptors
  PathDescriptor,
  PollInfo,
  PutResult,
  Result,
  SchemaResult,
  Ticket,
  // TLS
  TlsOptions
} from "./types.js"
export {
  cmdDescriptor,
  // Error class
  FlightError,
  fromCancelStatusProto,
  fromDescriptorTypeProto,
  // Helper functions
  pathDescriptor,
  // Proto conversion utils (internal but exported for advanced use)
  toCancelStatusProto,
  toDescriptorTypeProto,
  toFlightDescriptor
} from "./types.js"

// Client
export type { ConnectionState, HandshakeResult } from "./client.js"
export { createFlightClient, DoExchangeStream, DoPutStream, FlightClient } from "./client.js"

// Location utilities
export type { LocationScheme, ParsedLocation } from "./location.js"
export { createLocation, LocationParseError, parseLocation } from "./location.js"
