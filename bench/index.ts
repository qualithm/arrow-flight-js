/**
 * Benchmarks entry point.
 *
 * Run with: bun run bench
 */

/* eslint-disable no-console */

import { parseLocation } from "../src/location"

const ITERATIONS = 100_000

console.log("Running benchmarks...\n")

// Benchmark parsing plain gRPC URI
const grpcStart = performance.now()
for (let i = 0; i < ITERATIONS; i++) {
  parseLocation("grpc://localhost:8815")
}
const grpcEnd = performance.now()
const grpcTime = grpcEnd - grpcStart

console.log(`parseLocation (grpc://): ${ITERATIONS.toLocaleString()} iterations`)
console.log(`  Total: ${grpcTime.toFixed(2)}ms`)
console.log(`  Per call: ${((grpcTime / ITERATIONS) * 1000).toFixed(3)}μs`)
console.log()

// Benchmark parsing gRPC+TLS URI
const tlsStart = performance.now()
for (let i = 0; i < ITERATIONS; i++) {
  parseLocation("grpc+tls://flight.example.com:443")
}
const tlsEnd = performance.now()
const tlsTime = tlsEnd - tlsStart

console.log(`parseLocation (grpc+tls://): ${ITERATIONS.toLocaleString()} iterations`)
console.log(`  Total: ${tlsTime.toFixed(2)}ms`)
console.log(`  Per call: ${((tlsTime / ITERATIONS) * 1000).toFixed(3)}μs`)
console.log()

console.log("Benchmarks complete.")
