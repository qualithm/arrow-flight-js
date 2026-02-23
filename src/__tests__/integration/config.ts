/**
 * Integration test configuration.
 *
 * Configure the Arrow Flight server connection via environment variables:
 * - FLIGHT_HOST: Host address (default: localhost)
 * - FLIGHT_PORT: Port number (default: 50051)
 * - FLIGHT_TLS: Enable TLS (default: false)
 */

export const config = {
  host: process.env.FLIGHT_HOST ?? "localhost",
  port: parseInt(process.env.FLIGHT_PORT ?? "50051", 10),
  tls: process.env.FLIGHT_TLS === "true",

  // Test credentials (configure for your Flight server)
  credentials: {
    admin: { username: "admin", password: "admin123" },
    reader: { username: "reader", password: "reader123" },
    invalid: { username: "invalid", password: "wrong" }
  },

  // Test flight paths (configure for your Flight server)
  flights: {
    integers: ["test", "integers"],
    strings: ["test", "strings"],
    allTypes: ["test", "all-types"],
    empty: ["test", "empty"],
    large: ["test", "large"],
    nested: ["test", "nested"]
  }
} as const

/**
 * Checks if the Arrow Flight server is likely available.
 * This is a basic check - actual availability is confirmed by connection.
 */
export function isServerConfigured(): boolean {
  return config.host !== "" && config.port > 0
}
