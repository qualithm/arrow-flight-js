/**
 * Authentication example.
 *
 * Demonstrates different authentication methods for connecting
 * to a Flight server.
 *
 * @example
 * ```bash
 * bun run examples/authentication.ts
 * ```
 */
import { createFlightClient, FlightClient } from "../src/index.js"

async function main(): Promise<void> {
  console.log("=== Authentication Examples ===\n")

  // Example 1: No authentication (for local development)
  console.log("--- Example 1: No Authentication ---")
  await withNoAuth()

  // Example 2: Basic authentication (username/password)
  console.log("\n--- Example 2: Basic Authentication ---")
  await withBasicAuth()

  // Example 3: Bearer token authentication
  console.log("\n--- Example 3: Bearer Token Authentication ---")
  await withBearerAuth()

  // Example 4: Handshake authentication
  console.log("\n--- Example 4: Handshake Authentication ---")
  await withHandshake()

  // Example 5: Manual bearer token
  console.log("\n--- Example 5: Manual Bearer Token ---")
  await withManualToken()

  // Example 6: Raw handshake payload
  console.log("\n--- Example 6: Raw Handshake Payload ---")
  await withRawHandshakePayload()
}

async function withNoAuth(): Promise<void> {
  // Simple connection without authentication
  // Typically used for local development or testing
  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("  Connected without authentication")
  client.close()
  console.log("  Connection closed")
}

async function withBasicAuth(): Promise<void> {
  // Basic authentication with username and password
  // The credentials are sent with each request
  try {
    const client = await createFlightClient({
      host: "localhost",
      port: 8815,
      tls: false,
      auth: {
        type: "basic",
        username: "demo_user",
        password: "demo_password"
      }
    })

    console.log("  Connected with basic auth")
    client.close()
    console.log("  Connection closed")
  } catch (error) {
    console.log("  Connection failed (expected if server doesn't support basic auth)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function withBearerAuth(): Promise<void> {
  // Bearer token authentication
  // Commonly used with OAuth2 or JWT tokens
  try {
    const client = await createFlightClient({
      host: "localhost",
      port: 8815,
      tls: false,
      auth: {
        type: "bearer",
        token: "your-jwt-or-oauth-token"
      }
    })

    console.log("  Connected with bearer token")
    client.close()
    console.log("  Connection closed")
  } catch (error) {
    console.log("  Connection failed (expected if server doesn't support bearer auth)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function withHandshake(): Promise<void> {
  // Handshake authentication
  // Uses the Flight handshake RPC for authentication
  try {
    const client = new FlightClient({
      host: "localhost",
      port: 8815,
      tls: false,
      auth: {
        type: "basic",
        username: "demo_user",
        password: "demo_password"
      }
    })

    await client.connect()
    console.log("  Connected, performing handshake...")

    // Perform handshake - credentials are sent based on auth config
    const result = await client.handshake()

    console.log("  Handshake completed")
    console.log("  Protocol version:", result.protocolVersion)

    if (result.token !== undefined) {
      console.log("  Received token:", `${result.token.slice(0, 20)}...`)
      // Token is automatically set for subsequent requests
    }

    client.close()
    console.log("  Connection closed")
  } catch (error) {
    console.log("  Handshake failed (expected if server doesn't support it)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

async function withManualToken(): Promise<void> {
  // Manually set a bearer token after connection
  // Useful when obtaining tokens through external means
  const client = await createFlightClient({
    host: "localhost",
    port: 8815,
    tls: false
  })

  console.log("  Connected without auth")

  // Set bearer token manually
  const externalToken = "token-obtained-externally"
  client.setBearerToken(externalToken)
  console.log("  Bearer token set manually")

  // Check current token
  const currentToken = client.getBearerToken()
  console.log("  Current token:", currentToken)

  // Clear token if needed
  client.clearBearerToken()
  console.log("  Token cleared")

  client.close()
  console.log("  Connection closed")
}

async function withRawHandshakePayload(): Promise<void> {
  // For servers that use custom handshake protocols,
  // you can send a raw payload buffer
  try {
    const client = new FlightClient({
      host: "localhost",
      port: 8815,
      tls: false,
      auth: {
        type: "handshake",
        // Custom binary payload (e.g., serialised protobuf, JSON, etc.)
        payload: Buffer.from(
          JSON.stringify({
            apiKey: "your-api-key",
            clientId: "client-123",
            timestamp: Date.now()
          })
        )
      }
    })

    await client.connect()
    console.log("  Connected, performing custom handshake...")

    // The handshake sends the raw payload to the server
    const result = await client.handshake()

    console.log("  Custom handshake completed")
    console.log("  Response payload:", result.payload.length, "bytes")

    if (result.token !== undefined) {
      console.log("  Received token:", `${result.token.slice(0, 20)}...`)
    }

    client.close()
    console.log("  Connection closed")
  } catch (error) {
    console.log("  Custom handshake failed (expected if server doesn't support it)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
