/**
 * Arrow Flight client implementation.
 *
 * @packageDocumentation
 */

import {
  type ChannelCredentials,
  type ChannelOptions,
  type ClientReadableStream,
  credentials as grpcCredentials,
  Metadata
} from "@grpc/grpc-js"

import {
  BasicAuth,
  type Criteria,
  type FlightData,
  FlightServiceClient,
  type HandshakeResponse,
  type SchemaResult
} from "./generated/arrow/flight/protocol/Flight.js"
import {
  type CallOptions,
  type Descriptor,
  type FlightClientOptions,
  FlightError,
  type FlightInfo,
  type Ticket,
  type TlsOptions,
  toFlightDescriptor
} from "./types.js"

/**
 * Connection state for the Flight client.
 */
export type ConnectionState = "disconnected" | "connecting" | "connected" | "closed"

/**
 * Result of a successful handshake operation.
 */
export type HandshakeResult = {
  /**
   * The protocol version returned by the server.
   */
  protocolVersion: number

  /**
   * The response payload from the server.
   * For basic auth, this is typically a bearer token.
   */
  payload: Buffer

  /**
   * The extracted bearer token, if one was found.
   * This is automatically set on the client for subsequent requests.
   */
  token?: string
}

/**
 * Arrow Flight client for connecting to Flight servers.
 *
 * @example
 * ```ts
 * const client = new FlightClient({
 *   host: "localhost",
 *   port: 8815,
 *   tls: false
 * })
 *
 * await client.connect()
 *
 * // Use client methods...
 *
 * client.close()
 * ```
 */
export class FlightClient {
  private readonly options: FlightClientOptions
  private grpcClient: FlightServiceClient | null = null
  private _state: ConnectionState = "disconnected"
  private _bearerToken: string | null = null

  /**
   * Creates a new FlightClient instance.
   *
   * @param options - Connection options
   */
  constructor(options: FlightClientOptions) {
    this.options = options
  }

  /**
   * Current connection state.
   */
  get state(): ConnectionState {
    return this._state
  }

  /**
   * The target address in the format "host:port".
   */
  get address(): string {
    const port = this.options.port ?? (this.options.tls !== false ? 443 : 80)
    return `${this.options.host}:${String(port)}`
  }

  /**
   * Whether the client is currently connected.
   */
  get isConnected(): boolean {
    return this._state === "connected"
  }

  /**
   * Establishes a connection to the Flight server.
   *
   * This creates the underlying gRPC channel. For servers that require
   * authentication via Handshake, call `handshake()` after connecting.
   *
   * @throws {FlightError} If connection fails
   */
  async connect(): Promise<void> {
    if (this._state === "closed") {
      throw new FlightError("client has been closed", "FAILED_PRECONDITION")
    }

    if (this._state === "connected") {
      return
    }

    this._state = "connecting"

    try {
      const channelCredentials = this.buildCredentials()
      const channelOptions = this.buildChannelOptions()

      this.grpcClient = new FlightServiceClient(this.address, channelCredentials, channelOptions)

      // Wait for the channel to be ready
      await this.waitForReady()

      this._state = "connected"
    } catch (error) {
      this._state = "disconnected"
      throw this.wrapError(error)
    }
  }

  /**
   * Closes the connection to the Flight server.
   *
   * After calling close(), the client cannot be reconnected.
   * Create a new FlightClient instance if you need to reconnect.
   */
  close(): void {
    if (this.grpcClient) {
      this.grpcClient.close()
      this.grpcClient = null
    }
    this._state = "closed"
    this._bearerToken = null
  }

  /**
   * Gets the underlying gRPC client.
   *
   * @internal
   * @throws {FlightError} If not connected
   */
  getGrpcClient(): FlightServiceClient {
    if (!this.grpcClient || this._state !== "connected") {
      throw new FlightError("client is not connected", "FAILED_PRECONDITION")
    }
    return this.grpcClient
  }

  /**
   * Sets a bearer token for subsequent requests.
   *
   * This is typically set automatically after a successful handshake,
   * but can be set manually for pre-authenticated tokens.
   *
   * @param token - The bearer token
   */
  setBearerToken(token: string): void {
    this._bearerToken = token
  }

  /**
   * Gets the current bearer token, if set.
   */
  getBearerToken(): string | null {
    return this._bearerToken
  }

  /**
   * Clears the current bearer token.
   */
  clearBearerToken(): void {
    this._bearerToken = null
  }

  /**
   * Creates gRPC metadata for a call, including authentication headers.
   *
   * @internal
   * @param callOptions - Optional call-level options
   */
  createMetadata(callOptions?: CallOptions): Metadata {
    const metadata = new Metadata()

    // Add bearer token if set
    if (this._bearerToken !== null) {
      metadata.set("authorization", `Bearer ${this._bearerToken}`)
    }

    // Add basic auth if configured
    const { auth } = this.options
    if (auth?.type === "basic") {
      const encoded = Buffer.from(`${auth.username}:${auth.password}`).toString("base64")
      metadata.set("authorization", `Basic ${encoded}`)
    } else if (auth?.type === "bearer") {
      metadata.set("authorization", `Bearer ${auth.token}`)
    }

    // Add custom headers from call options
    if (callOptions?.headers !== undefined) {
      for (const [key, value] of Object.entries(callOptions.headers)) {
        metadata.set(key, value)
      }
    }

    return metadata
  }

  /**
   * Performs a handshake with the Flight server for authentication.
   *
   * The handshake mechanism depends on the configured auth options:
   * - `basic`: Sends username/password encoded as BasicAuth protobuf
   * - `handshake`: Sends the raw payload buffer
   * - No auth: Sends an empty handshake (useful for servers that return session tokens)
   *
   * On success, if the server returns a bearer token (in the response payload
   * or metadata), it is automatically set for subsequent requests.
   *
   * @param callOptions - Optional call-level options
   * @returns The handshake result including protocol version and any token
   * @throws {FlightError} If the handshake fails
   *
   * @example
   * ```ts
   * const client = new FlightClient({
   *   host: "localhost",
   *   port: 8815,
   *   tls: false,
   *   auth: { type: "basic", username: "user", password: "pass" }
   * })
   *
   * await client.connect()
   * const result = await client.handshake()
   * console.log("Token:", result.token)
   * ```
   */
  async handshake(callOptions?: CallOptions): Promise<HandshakeResult> {
    const grpcClient = this.getGrpcClient()
    const metadata = this.createMetadata(callOptions)

    // Build the handshake payload based on auth configuration
    const payload = this.buildHandshakePayload()

    return new Promise((resolve, reject) => {
      const stream = grpcClient.handshake(metadata)
      let response: HandshakeResponse | null = null
      let extractedToken: string | undefined

      stream.on("data", (data: HandshakeResponse) => {
        response = data
      })

      stream.on("metadata", (meta: Metadata) => {
        // Check for token in response metadata
        const authHeader = meta.get("authorization")
        if (authHeader.length > 0) {
          const authValue = String(authHeader[0])
          if (authValue.toLowerCase().startsWith("bearer ")) {
            extractedToken = authValue.slice(7)
          }
        }

        // Some servers use auth-token-bin
        const tokenBin = meta.get("auth-token-bin")
        if (tokenBin.length > 0 && extractedToken === undefined) {
          const tokenValue = tokenBin[0]
          if (typeof tokenValue === "string") {
            extractedToken = tokenValue
          } else if (Buffer.isBuffer(tokenValue)) {
            extractedToken = tokenValue.toString("utf8")
          }
        }
      })

      stream.on("error", (error: Error) => {
        reject(this.wrapError(error))
      })

      stream.on("end", () => {
        if (response === null) {
          reject(new FlightError("no handshake response received", "INTERNAL"))
          return
        }

        // Try to extract token from response payload if not found in metadata
        if (extractedToken === undefined && response.payload.length > 0) {
          // Response payload is often the raw token string
          extractedToken = response.payload.toString("utf8")
        }

        // Set the bearer token for subsequent requests
        if (extractedToken !== undefined) {
          this.setBearerToken(extractedToken)
        }

        resolve({
          protocolVersion: response.protocolVersion,
          payload: response.payload,
          token: extractedToken
        })
      })

      // Send the handshake request
      stream.write({
        protocolVersion: 0,
        payload
      })
      stream.end()
    })
  }

  /**
   * Lists available flights matching the given criteria.
   *
   * This returns an async iterable that yields FlightInfo objects describing
   * available data streams. The criteria parameter is server-specific and can
   * be used to filter or limit the results.
   *
   * @param criteria - Optional filter criteria (server-specific)
   * @param callOptions - Optional call-level options
   * @returns An async iterable of FlightInfo objects
   * @throws {FlightError} If the operation fails
   *
   * @example
   * ```ts
   * for await (const info of client.listFlights()) {
   *   console.log("Flight:", info.flightDescriptor)
   * }
   * ```
   */
  async *listFlights(
    criteria?: Criteria,
    callOptions?: CallOptions
  ): AsyncGenerator<FlightInfo, void, undefined> {
    const grpcClient = this.getGrpcClient()
    const metadata = this.createMetadata(callOptions)

    const request: Criteria = criteria ?? { expression: Buffer.alloc(0) }
    const stream = grpcClient.listFlights(request, metadata)

    yield* this.streamToAsyncIterable<FlightInfo>(stream)
  }

  /**
   * Gets information about a specific flight.
   *
   * Returns metadata about a flight including its schema, endpoints for
   * data retrieval, and size estimates. The descriptor identifies the data
   * either by path or command.
   *
   * @param descriptor - The flight descriptor (path or command)
   * @param callOptions - Optional call-level options
   * @returns Information about the flight
   * @throws {FlightError} If the flight is not found or the operation fails
   *
   * @example
   * ```ts
   * import { pathDescriptor } from "arrow-flight-js"
   *
   * const info = await client.getFlightInfo(pathDescriptor("my", "dataset"))
   * console.log("Schema:", info.schema)
   * console.log("Endpoints:", info.endpoint.length)
   * ```
   */
  async getFlightInfo(descriptor: Descriptor, callOptions?: CallOptions): Promise<FlightInfo> {
    const grpcClient = this.getGrpcClient()
    const metadata = this.createMetadata(callOptions)
    const request = toFlightDescriptor(descriptor)

    return new Promise((resolve, reject) => {
      grpcClient.getFlightInfo(request, metadata, (error, response) => {
        if (error !== null) {
          reject(this.wrapError(error))
        } else {
          resolve(response)
        }
      })
    })
  }

  /**
   * Gets the Arrow schema for a specific flight.
   *
   * Returns the schema without endpoint information. This is useful when
   * you only need the schema and don't need to know where to retrieve the data.
   *
   * @param descriptor - The flight descriptor (path or command)
   * @param callOptions - Optional call-level options
   * @returns The schema result containing the IPC-encoded schema
   * @throws {FlightError} If the flight is not found or the operation fails
   *
   * @example
   * ```ts
   * import { cmdDescriptor } from "arrow-flight-js"
   *
   * const result = await client.getSchema(cmdDescriptor(Buffer.from("SELECT 1")))
   * console.log("Schema bytes:", result.schema.length)
   * ```
   */
  async getSchema(descriptor: Descriptor, callOptions?: CallOptions): Promise<SchemaResult> {
    const grpcClient = this.getGrpcClient()
    const metadata = this.createMetadata(callOptions)
    const request = toFlightDescriptor(descriptor)

    return new Promise((resolve, reject) => {
      grpcClient.getSchema(request, metadata, (error, response) => {
        if (error !== null) {
          reject(this.wrapError(error))
        } else {
          resolve(response)
        }
      })
    })
  }

  /**
   * Retrieves data for a flight ticket.
   *
   * Returns an async iterable that yields FlightData messages containing
   * Arrow IPC-encoded data. The ticket is obtained from a FlightEndpoint,
   * which is part of a FlightInfo returned by getFlightInfo() or listFlights().
   *
   * @param ticket - The ticket identifying the data stream
   * @param callOptions - Optional call-level options
   * @returns An async iterable of FlightData messages
   * @throws {FlightError} If the ticket is invalid or the operation fails
   *
   * @example
   * ```ts
   * const info = await client.getFlightInfo(pathDescriptor("my", "data"))
   * for (const endpoint of info.endpoint) {
   *   for await (const data of client.doGet(endpoint.ticket!)) {
   *     console.log("Received:", data.dataBody.length, "bytes")
   *   }
   * }
   * ```
   */
  async *doGet(
    ticket: Ticket,
    callOptions?: CallOptions
  ): AsyncGenerator<FlightData, void, undefined> {
    const grpcClient = this.getGrpcClient()
    const metadata = this.createMetadata(callOptions)

    const stream = grpcClient.doGet(ticket, metadata)

    yield* this.streamToAsyncIterable<FlightData>(stream)
  }

  /**
   * Converts a gRPC readable stream to an async iterable.
   *
   * @internal
   */
  private async *streamToAsyncIterable<T>(
    stream: ClientReadableStream<T>
  ): AsyncGenerator<T, void, undefined> {
    type QueueItem = { type: "data"; value: T } | { type: "error"; value: Error } | { type: "end" }
    const queue: QueueItem[] = []
    let notify: (() => void) | null = null

    const push = (item: QueueItem): void => {
      queue.push(item)
      if (notify !== null) {
        notify()
        notify = null
      }
    }

    stream.on("data", (data: T) => {
      push({ type: "data", value: data })
    })

    stream.on("error", (err: Error) => {
      push({ type: "error", value: this.wrapError(err) })
    })

    stream.on("end", () => {
      push({ type: "end" })
    })

    let done = false
    while (!done) {
      while (queue.length === 0) {
        await new Promise<void>((r) => {
          notify = r
        })
      }

      // Queue is guaranteed to have items after the inner while loop
      // Use index access and then mutate to avoid non-null assertion
      const item = queue[0]
      queue.splice(0, 1)

      switch (item.type) {
        case "data":
          yield item.value
          break
        case "error":
          throw item.value
        case "end":
          done = true
          break
      }
    }
  }

  /**
   * Builds the handshake payload based on auth configuration.
   */
  private buildHandshakePayload(): Buffer {
    const { auth } = this.options

    if (auth?.type === "basic") {
      // Encode as BasicAuth protobuf
      const basicAuth = { username: auth.username, password: auth.password }
      return Buffer.from(BasicAuth.encode(basicAuth).finish())
    }

    if (auth?.type === "handshake") {
      return auth.payload
    }

    // Empty payload for other auth types or no auth
    return Buffer.alloc(0)
  }

  /**
   * Builds gRPC channel credentials from client options.
   */
  private buildCredentials(): ChannelCredentials {
    // If explicit credentials provided, use them
    if (this.options.credentials !== undefined) {
      return this.options.credentials
    }

    // Handle mTLS via auth option (legacy support)
    const { auth } = this.options
    if (auth?.type === "mtls") {
      const cert = typeof auth.cert === "string" ? Buffer.from(auth.cert) : auth.cert
      const key = typeof auth.key === "string" ? Buffer.from(auth.key) : auth.key
      const ca =
        auth.ca !== undefined
          ? typeof auth.ca === "string"
            ? Buffer.from(auth.ca)
            : auth.ca
          : undefined

      return grpcCredentials.createSsl(ca, key, cert)
    }

    // Handle TLS options
    const { tls } = this.options

    // Explicit false means insecure
    if (tls === false) {
      return grpcCredentials.createInsecure()
    }

    // If tls is an object, use its configuration
    if (typeof tls === "object") {
      return this.buildTlsCredentials(tls)
    }

    // Default: TLS with system CAs
    return grpcCredentials.createSsl()
  }

  /**
   * Builds TLS credentials from TlsOptions.
   */
  private buildTlsCredentials(tlsOptions: TlsOptions): ChannelCredentials {
    const rootCerts =
      tlsOptions.rootCerts !== undefined
        ? typeof tlsOptions.rootCerts === "string"
          ? Buffer.from(tlsOptions.rootCerts)
          : tlsOptions.rootCerts
        : undefined

    const privateKey =
      tlsOptions.privateKey !== undefined
        ? typeof tlsOptions.privateKey === "string"
          ? Buffer.from(tlsOptions.privateKey)
          : tlsOptions.privateKey
        : undefined

    const certChain =
      tlsOptions.certChain !== undefined
        ? typeof tlsOptions.certChain === "string"
          ? Buffer.from(tlsOptions.certChain)
          : tlsOptions.certChain
        : undefined

    // grpc-js createSsl signature: (rootCerts?, privateKey?, certChain?, verifyOptions?)
    const verifyOptions =
      tlsOptions.verifyServerCert === false ? { checkServerIdentity: () => undefined } : undefined

    return grpcCredentials.createSsl(rootCerts, privateKey, certChain, verifyOptions)
  }

  /**
   * Builds gRPC channel options from client options.
   */
  private buildChannelOptions(): ChannelOptions {
    const options: ChannelOptions = {}
    const { channelOptions: channelOpts } = this.options

    if (channelOpts?.maxReceiveMessageLength !== undefined) {
      options["grpc.max_receive_message_length"] = channelOpts.maxReceiveMessageLength
    }

    if (channelOpts?.maxSendMessageLength !== undefined) {
      options["grpc.max_send_message_length"] = channelOpts.maxSendMessageLength
    }

    if (channelOpts?.keepaliveTimeMs !== undefined) {
      options["grpc.keepalive_time_ms"] = channelOpts.keepaliveTimeMs
    }

    if (channelOpts?.keepaliveTimeoutMs !== undefined) {
      options["grpc.keepalive_timeout_ms"] = channelOpts.keepaliveTimeoutMs
    }

    if (channelOpts?.keepalivePermitWithoutCalls !== undefined) {
      options["grpc.keepalive_permit_without_calls"] = channelOpts.keepalivePermitWithoutCalls
        ? 1
        : 0
    }

    // Add SSL target name override from TLS options
    const { tls } = this.options
    if (typeof tls === "object" && tls.serverNameOverride !== undefined) {
      options["grpc.ssl_target_name_override"] = tls.serverNameOverride
    }

    return options
  }

  /**
   * Waits for the gRPC channel to be ready.
   */
  private async waitForReady(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.grpcClient === null) {
        reject(new Error("client not initialised"))
        return
      }

      const timeout = this.options.channelOptions?.connectTimeoutMs ?? 30000
      const deadline = Date.now() + timeout

      this.grpcClient.waitForReady(deadline, (error) => {
        if (error) {
          reject(error)
        } else {
          resolve()
        }
      })
    })
  }

  /**
   * Wraps an error in a FlightError.
   */
  private wrapError(error: unknown): FlightError {
    if (error instanceof FlightError) {
      return error
    }

    if (error instanceof Error) {
      return new FlightError(error.message, "UNKNOWN", error.stack)
    }

    return new FlightError(String(error), "UNKNOWN")
  }
}

/**
 * Creates a new FlightClient and connects to the server.
 *
 * This is a convenience function that combines creating a client
 * and calling connect() in one step.
 *
 * @param options - Connection options
 * @returns A connected FlightClient
 *
 * @example
 * ```ts
 * const client = await createFlightClient({
 *   host: "localhost",
 *   port: 8815,
 *   tls: false
 * })
 * ```
 */
export async function createFlightClient(options: FlightClientOptions): Promise<FlightClient> {
  const client = new FlightClient(options)
  await client.connect()
  return client
}
