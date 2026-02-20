/**
 * Authentication and TLS configuration example.
 *
 * Demonstrates different authentication methods and TLS configurations
 * for connecting to a Flight server.
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

  // TLS Configuration Examples
  console.log("\n=== TLS Configuration Examples ===\n")

  // Example 7: TLS with system CAs
  console.log("--- Example 7: Default TLS ---")
  showDefaultTls()

  // Example 8: Custom root CA
  console.log("\n--- Example 8: Custom Root CA ---")
  showCustomRootCa()

  // Example 9: Mutual TLS (mTLS)
  console.log("\n--- Example 9: Mutual TLS (mTLS) ---")
  showMtls()
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

// =============================================================================
// TLS Configuration Functions
// =============================================================================

function showDefaultTls(): void {
  // When connecting to a server with a certificate signed by a public CA,
  // TLS works out of the box - no configuration needed
  console.log("  Code example:")
  console.log("")
  console.log("  const client = await createFlightClient({")
  console.log('    host: "flight.example.com",')
  console.log("    port: 443")
  console.log("    // tls: true is the default")
  console.log("  })")
  console.log("")
  console.log("  // The system's default CA bundle is used for verification")
}

function showCustomRootCa(): void {
  // For servers with certificates signed by internal/private CAs,
  // provide the root CA certificate
  console.log("  Code example:")
  console.log("")
  console.log('  import fs from "fs"')
  console.log("")
  console.log("  const client = await createFlightClient({")
  console.log('    host: "flight.internal.company.com",')
  console.log("    port: 8815,")
  console.log("    tls: {")
  console.log("      // Provide your organization's root CA")
  console.log('      rootCerts: fs.readFileSync("/path/to/ca.crt")')
  console.log("    }")
  console.log("  })")
  console.log("")
  console.log("  // Alternatively, pass as a string:")
  console.log("  // rootCerts: `-----BEGIN CERTIFICATE-----\\n...`")
}

function showMtls(): void {
  // Mutual TLS requires both client certificate and private key
  // The server verifies the client's identity
  console.log("  Code example:")
  console.log("")
  console.log('  import fs from "fs"')
  console.log("")
  console.log("  const client = await createFlightClient({")
  console.log('    host: "flight.secure.example.com",')
  console.log("    port: 443,")
  console.log("    tls: {")
  console.log('      rootCerts: fs.readFileSync("ca.crt"),')
  console.log('      certChain: fs.readFileSync("client.crt"),')
  console.log('      privateKey: fs.readFileSync("client.key")')
  console.log("    }")
  console.log("  })")
  console.log("")
  console.log("  // Or using the auth.mtls shorthand:")
  console.log("  // auth: { type: 'mtls', cert, key, ca }")
}

main().catch(console.error)
