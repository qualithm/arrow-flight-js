/**
 * TLS configuration example.
 *
 * Demonstrates different TLS/SSL configurations for secure
 * connections to Flight servers.
 *
 * @example
 * ```bash
 * bun run examples/tls-configuration.ts
 * ```
 */
import { createFlightClient } from "../src/index.js"

async function main(): Promise<void> {
  console.log("=== TLS Configuration Examples ===\n")

  // Example 1: Default TLS (system CAs)
  console.log("--- Example 1: Default TLS ---")
  showDefaultTls()

  // Example 2: Custom root CA
  console.log("\n--- Example 2: Custom Root CA ---")
  showCustomRootCa()

  // Example 3: mTLS (mutual TLS)
  console.log("\n--- Example 3: Mutual TLS (mTLS) ---")
  showMtls()

  // Example 4: Server name override
  console.log("\n--- Example 4: Server Name Override ---")
  showServerNameOverride()

  // Example 5: Insecure connection
  console.log("\n--- Example 5: Insecure Connection ---")
  await showInsecure()
}

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
  console.log("  // Option 1: Using tls configuration")
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
  console.log("  // Option 2: Using auth configuration (legacy)")
  console.log("  const client = await createFlightClient({")
  console.log('    host: "flight.secure.example.com",')
  console.log("    port: 443,")
  console.log("    auth: {")
  console.log('      type: "mtls",')
  console.log('      cert: fs.readFileSync("client.crt"),')
  console.log('      key: fs.readFileSync("client.key"),')
  console.log('      ca: fs.readFileSync("ca.crt")')
  console.log("    }")
  console.log("  })")
}

function showServerNameOverride(): void {
  // When connecting via IP or through a proxy, you may need to
  // override the server name for certificate verification
  console.log("  Code example:")
  console.log("")
  console.log("  const client = await createFlightClient({")
  console.log('    host: "192.168.1.100", // Connecting by IP')
  console.log("    port: 8815,")
  console.log("    tls: {")
  console.log('      rootCerts: fs.readFileSync("ca.crt"),')
  console.log("      // The certificate is issued for this hostname")
  console.log('      serverNameOverride: "flight.example.com"')
  console.log("    }")
  console.log("  })")
  console.log("")
  console.log("  // This allows connecting by IP while verifying the")
  console.log('  // certificate against "flight.example.com"')
}

async function showInsecure(): Promise<void> {
  // For local development or testing, disable TLS entirely
  // WARNING: Never use in production!
  console.log("  WARNING: Insecure connections should only be used for development!")
  console.log("")

  try {
    const client = await createFlightClient({
      host: "localhost",
      port: 8815,
      tls: false // Explicitly disable TLS
    })

    console.log("  Connected without TLS (insecure)")
    console.log("  Address:", client.address)

    client.close()
    console.log("  Connection closed")
  } catch (error) {
    console.log("  Connection failed (expected if no server running)")
    console.log("  Error:", error instanceof Error ? error.message : error)
  }
}

main().catch(console.error)
