/**
 * Arrow Flight client benchmarks.
 *
 * Run with: FLIGHT_HOST=localhost FLIGHT_PORT=50051 bun run bench
 *
 * Requires a running Arrow Flight server with test fixtures.
 */

/* eslint-disable no-console */

import { tableFromIPC } from "apache-arrow"

import {
  collectFlightDataAsIpc,
  createFlightClient,
  type FlightClient,
  parseLocation,
  pathDescriptor
} from "../src/index"

const config = {
  host: process.env.FLIGHT_HOST ?? "localhost",
  port: parseInt(process.env.FLIGHT_PORT ?? "50051", 10),
  tls: process.env.FLIGHT_TLS === "true",
  username: process.env.FLIGHT_USER ?? "admin",
  password: process.env.FLIGHT_PASSWORD ?? "admin123"
}

const WARMUP_ITERATIONS = 15
const BENCHMARK_ITERATIONS = 100

type BenchmarkResult = {
  name: string
  iterations: number
  totalMs: number
  avgMs: number
  minMs: number
  maxMs: number
  stdDev: number
  cv: number // coefficient of variation (%)
  p50Ms: number
  p95Ms: number
  p99Ms: number
  rowsPerSec?: number
  bytesPerSec?: number
  opsPerSec?: number
  memoryDeltaBytes?: number
  skipped?: boolean
}

type QpsResult = {
  name: string
  durationMs: number
  totalQueries: number
  qps: number
  concurrency: number
  avgLatencyMs: number
  p50LatencyMs: number
  p99LatencyMs: number
  errors: number
}

const QPS_DURATION_SEC = 10
const QPS_CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32]

function percentile(sortedArr: number[], p: number): number {
  const index = Math.ceil((p / 100) * sortedArr.length) - 1
  return sortedArr[Math.max(0, index)]
}

function formatResult(result: BenchmarkResult): void {
  if (result.skipped === true) {
    console.log(`${result.name}: [SKIPPED]\n`)
    return
  }
  const stability = result.cv > 30 ? " [UNSTABLE]" : ""
  console.log(`${result.name}:${stability}`)
  console.log(`  Iterations: ${String(result.iterations)}`)
  console.log(`  Total: ${result.totalMs.toFixed(2)}ms`)
  console.log(
    `  Avg: ${result.avgMs.toFixed(3)}ms (±${result.stdDev.toFixed(3)}ms, CV: ${result.cv.toFixed(1)}%)`
  )
  console.log(`  Min: ${result.minMs.toFixed(3)}ms`)
  console.log(`  Max: ${result.maxMs.toFixed(3)}ms`)
  console.log(`  P50: ${result.p50Ms.toFixed(3)}ms`)
  console.log(`  P95: ${result.p95Ms.toFixed(3)}ms`)
  console.log(`  P99: ${result.p99Ms.toFixed(3)}ms`)
  if (result.opsPerSec !== undefined) {
    console.log(`  Ops/sec: ${result.opsPerSec.toLocaleString()}`)
  }
  if (result.rowsPerSec !== undefined) {
    console.log(`  Throughput: ${result.rowsPerSec.toLocaleString()} rows/sec`)
  }
  if (result.bytesPerSec !== undefined) {
    const mbPerSec = result.bytesPerSec / (1024 * 1024)
    console.log(`  Bandwidth: ${mbPerSec.toFixed(2)} MB/sec`)
  }
  if (result.memoryDeltaBytes !== undefined) {
    const mb = result.memoryDeltaBytes / (1024 * 1024)
    console.log(`  Memory delta: ${mb.toFixed(2)} MB`)
  }
  console.log()
}

function triggerGC(): void {
  if (typeof Bun !== "undefined") {
    Bun.gc(true)
  } else if (typeof globalThis.gc === "function") {
    globalThis.gc()
  }
}

type BenchmarkMetrics = {
  rows?: number
  bytes?: number
}

async function benchmark(
  name: string,
  fn: () => Promise<number | BenchmarkMetrics | undefined>,
  iterations: number = BENCHMARK_ITERATIONS,
  options: { trackMemory?: boolean } = {}
): Promise<BenchmarkResult> {
  triggerGC()

  for (let i = 0; i < WARMUP_ITERATIONS; i++) {
    await fn()
  }

  triggerGC()

  const memBefore = options.trackMemory === true ? process.memoryUsage().heapUsed : 0
  const times: number[] = []
  let totalRows = 0
  let totalBytes = 0

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const result = await fn()
    const end = performance.now()
    times.push(end - start)
    if (typeof result === "number") {
      totalRows += result
    } else if (result && typeof result === "object") {
      if (result.rows !== undefined) {
        totalRows += result.rows
      }
      if (result.bytes !== undefined) {
        totalBytes += result.bytes
      }
    }
  }

  const memAfter = options.trackMemory === true ? process.memoryUsage().heapUsed : 0
  const memoryDeltaBytes = options.trackMemory === true ? memAfter - memBefore : undefined

  times.sort((a, b) => a - b)

  const totalMs = times.reduce((a, b) => a + b, 0)
  const avgMs = totalMs / iterations
  const variance = times.reduce((sum, t) => sum + Math.pow(t - avgMs, 2), 0) / iterations
  const stdDev = Math.sqrt(variance)
  const cv = avgMs > 0 ? (stdDev / avgMs) * 100 : 0
  const rowsPerSec = totalRows > 0 ? (totalRows / totalMs) * 1000 : undefined
  const bytesPerSec = totalBytes > 0 ? (totalBytes / totalMs) * 1000 : undefined
  const opsPerSec = (iterations / totalMs) * 1000

  return {
    name,
    iterations,
    totalMs,
    avgMs,
    minMs: times[0],
    maxMs: times[times.length - 1],
    stdDev,
    cv,
    p50Ms: percentile(times, 50),
    p95Ms: percentile(times, 95),
    p99Ms: percentile(times, 99),
    rowsPerSec,
    bytesPerSec,
    opsPerSec,
    memoryDeltaBytes
  }
}

function benchmarkSync(
  name: string,
  fn: () => number | undefined,
  iterations: number = BENCHMARK_ITERATIONS
): BenchmarkResult {
  triggerGC()

  for (let i = 0; i < WARMUP_ITERATIONS; i++) {
    fn()
  }

  triggerGC()

  const times: number[] = []

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    fn()
    const end = performance.now()
    times.push(end - start)
  }

  times.sort((a, b) => a - b)

  const totalMs = times.reduce((a, b) => a + b, 0)
  const avgMs = totalMs / iterations
  const variance = times.reduce((sum, t) => sum + Math.pow(t - avgMs, 2), 0) / iterations
  const stdDev = Math.sqrt(variance)
  const cv = avgMs > 0 ? (stdDev / avgMs) * 100 : 0
  const opsPerSec = (iterations / totalMs) * 1000

  return {
    name,
    iterations,
    totalMs,
    avgMs,
    minMs: times[0],
    maxMs: times[times.length - 1],
    stdDev,
    cv,
    p50Ms: percentile(times, 50),
    p95Ms: percentile(times, 95),
    p99Ms: percentile(times, 99),
    opsPerSec
  }
}

/**
 * Run a benchmark that may fail (e.g., missing flight).
 * Returns a skipped result instead of throwing.
 */
async function benchmarkOptional(
  name: string,
  fn: () => Promise<number | BenchmarkMetrics | undefined>,
  iterations: number = BENCHMARK_ITERATIONS,
  options: { trackMemory?: boolean } = {}
): Promise<BenchmarkResult> {
  try {
    return await benchmark(name, fn, iterations, options)
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    console.log(`SKIPPED: ${name} - ${message}`)
    return {
      name,
      iterations: 0,
      totalMs: 0,
      avgMs: 0,
      minMs: 0,
      maxMs: 0,
      stdDev: 0,
      cv: 0,
      p50Ms: 0,
      p95Ms: 0,
      p99Ms: 0,
      skipped: true
    }
  }
}

/**
 * Run a sustained QPS benchmark for a fixed duration with configurable concurrency.
 * Returns throughput and latency metrics under sustained load.
 */
async function benchmarkQps(
  name: string,
  fn: () => Promise<void>,
  options: { durationSec?: number; concurrency?: number } = {}
): Promise<QpsResult> {
  const durationMs = (options.durationSec ?? QPS_DURATION_SEC) * 1000
  const concurrency = options.concurrency ?? 1

  const latencies: number[] = []
  let completed = 0
  let errors = 0
  let running = true

  triggerGC()

  const startTime = performance.now()
  const deadline = startTime + durationMs

  const worker = async (): Promise<void> => {
    while (running && performance.now() < deadline) {
      const start = performance.now()
      try {
        await fn()
        latencies.push(performance.now() - start)
        completed++
      } catch {
        errors++
      }
    }
  }

  const workers = Array.from({ length: concurrency }, async () => worker())
  await Promise.all(workers)
  running = false

  const elapsed = performance.now() - startTime
  latencies.sort((a, b) => a - b)

  return {
    name,
    durationMs: elapsed,
    totalQueries: completed,
    qps: (completed / elapsed) * 1000,
    concurrency,
    avgLatencyMs:
      latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0,
    p50LatencyMs: latencies.length > 0 ? percentile(latencies, 50) : 0,
    p99LatencyMs: latencies.length > 0 ? percentile(latencies, 99) : 0,
    errors
  }
}

function formatQpsResult(result: QpsResult): void {
  const errorRate =
    result.totalQueries > 0 ? (result.errors / (result.totalQueries + result.errors)) * 100 : 0
  console.log(
    `  C=${String(result.concurrency).padStart(2)} | ` +
      `QPS: ${result.qps.toFixed(0).padStart(6)} | ` +
      `Avg: ${result.avgLatencyMs.toFixed(2).padStart(7)}ms | ` +
      `P50: ${result.p50LatencyMs.toFixed(2).padStart(7)}ms | ` +
      `P99: ${result.p99LatencyMs.toFixed(2).padStart(7)}ms | ` +
      `Errors: ${String(result.errors).padStart(4)} (${errorRate.toFixed(1)}%)`
  )
}

/**
 * Run QPS sweep across concurrency levels to find throughput ceiling.
 */
async function runQpsSweep(
  name: string,
  fn: () => Promise<void>,
  concurrencyLevels: number[] = QPS_CONCURRENCY_LEVELS
): Promise<QpsResult[]> {
  console.log(`\n${name}:`)
  console.log("  Concurrency | QPS | Avg Latency | P50 Latency | P99 Latency | Errors")

  const results: QpsResult[] = []

  for (const concurrency of concurrencyLevels) {
    const result = await benchmarkQps(name, fn, {
      durationSec: QPS_DURATION_SEC,
      concurrency
    })
    formatQpsResult(result)
    results.push(result)

    // Stop if error rate exceeds 5%
    const errorRate =
      result.totalQueries > 0 ? result.errors / (result.totalQueries + result.errors) : 0
    if (errorRate > 0.05) {
      console.log("  (stopping sweep due to high error rate)")
      break
    }
  }

  return results
}

async function runBenchmarks(): Promise<void> {
  console.log("Arrow Flight Client Benchmarks")
  console.log("================================")
  console.log(`Server: ${config.host}:${String(config.port)}`)
  console.log(`TLS: ${String(config.tls)}`)
  console.log(`Warmup: ${String(WARMUP_ITERATIONS)} iterations`)
  console.log(`Benchmark: ${String(BENCHMARK_ITERATIONS)} iterations`)
  console.log()

  const results: BenchmarkResult[] = []

  // ================================
  // Synchronous utility benchmarks
  // ================================
  console.log("Running synchronous utility benchmarks...")

  results.push(
    benchmarkSync(
      "parseLocation (grpc://)",
      () => {
        for (let i = 0; i < 1000; i++) {
          parseLocation("grpc://localhost:8815")
        }
        return 1000
      },
      100
    )
  )

  results.push(
    benchmarkSync(
      "parseLocation (grpc+tls://)",
      () => {
        for (let i = 0; i < 1000; i++) {
          parseLocation("grpc+tls://flight.example.com:443")
        }
        return 1000
      },
      100
    )
  )

  results.push(
    benchmarkSync(
      "pathDescriptor creation",
      () => {
        for (let i = 0; i < 1000; i++) {
          pathDescriptor("test", "integers")
        }
        return 1000
      },
      100
    )
  )

  // ================================
  // Connection benchmarks
  // ================================
  console.log("\nConnecting to Flight server...")
  const client: FlightClient = await createFlightClient({
    host: config.host,
    port: config.port,
    tls: config.tls,
    auth: {
      type: "basic",
      username: config.username,
      password: config.password
    }
  })

  await client.handshake()
  console.log("Connected.\n")

  console.log("Running connection benchmarks...")

  results.push(
    await benchmark(
      "Connection establishment",
      async () => {
        const c = await createFlightClient({
          host: config.host,
          port: config.port,
          tls: config.tls,
          auth: {
            type: "basic",
            username: config.username,
            password: config.password
          }
        })
        await c.handshake()
        c.close()
        return undefined
      },
      20
    )
  )

  results.push(
    await benchmark(
      "Handshake only (reuse connection)",
      async () => {
        const c = await createFlightClient({
          host: config.host,
          port: config.port,
          tls: config.tls,
          auth: {
            type: "basic",
            username: config.username,
            password: config.password
          }
        })
        await c.handshake()
        c.close()
        return undefined
      },
      20
    )
  )

  // ================================
  // Flight listing benchmarks
  // ================================
  console.log("\nRunning flight listing benchmarks...")

  results.push(
    await benchmark("listFlights (all)", async () => {
      let count = 0
      for await (const _info of client.listFlights()) {
        count++
      }
      return count
    })
  )

  results.push(
    await benchmark("listActions", async () => {
      let count = 0
      for await (const _action of client.listActions()) {
        count++
      }
      return count
    })
  )

  // ================================
  // FlightInfo benchmarks
  // ================================
  console.log("\nRunning FlightInfo benchmarks...")

  results.push(
    await benchmark("getFlightInfo (test/integers)", async () => {
      const info = await client.getFlightInfo(pathDescriptor("test", "integers"))
      return info.endpoint.length
    })
  )

  results.push(
    await benchmark("getFlightInfo (test/large)", async () => {
      const info = await client.getFlightInfo(pathDescriptor("test", "large"))
      return info.endpoint.length
    })
  )

  results.push(
    await benchmarkOptional("getSchema (test/integers)", async () => {
      const schema = await client.getSchema(pathDescriptor("test", "integers"))
      return schema.schema.length
    })
  )

  // ================================
  // DoGet benchmarks
  // ================================
  console.log("\nRunning DoGet benchmarks...")

  results.push(
    await benchmark("doGet test/integers (100 rows)", async () => {
      const info = await client.getFlightInfo(pathDescriptor("test", "integers"))
      const { ticket } = info.endpoint[0]
      if (ticket === undefined) {
        throw new Error("No ticket")
      }
      const ipcBytes = await collectFlightDataAsIpc(client.doGet(ticket))
      const table = tableFromIPC(ipcBytes)
      return table.numRows
    })
  )

  results.push(
    await benchmark("doGet test/strings (100 rows)", async () => {
      const info = await client.getFlightInfo(pathDescriptor("test", "strings"))
      const { ticket } = info.endpoint[0]
      if (ticket === undefined) {
        throw new Error("No ticket")
      }
      const ipcBytes = await collectFlightDataAsIpc(client.doGet(ticket))
      const table = tableFromIPC(ipcBytes)
      return table.numRows
    })
  )

  results.push(
    await benchmark(
      "doGet test/large (10000 rows)",
      async () => {
        const info = await client.getFlightInfo(pathDescriptor("test", "large"))
        const { ticket } = info.endpoint[0]
        if (ticket === undefined) {
          throw new Error("No ticket")
        }
        const ipcBytes = await collectFlightDataAsIpc(client.doGet(ticket))
        const table = tableFromIPC(ipcBytes)
        return table.numRows
      },
      20
    )
  )

  results.push(
    await benchmarkOptional("doGet test/nested (50 rows)", async () => {
      const info = await client.getFlightInfo(pathDescriptor("test", "nested"))
      const { ticket } = info.endpoint[0]
      if (ticket === undefined) {
        throw new Error("No ticket")
      }
      const ipcBytes = await collectFlightDataAsIpc(client.doGet(ticket))
      const table = tableFromIPC(ipcBytes)
      return table.numRows
    })
  )

  results.push(
    await benchmarkOptional("doGet test/empty (0 rows)", async () => {
      const info = await client.getFlightInfo(pathDescriptor("test", "empty"))
      const { ticket } = info.endpoint[0]
      if (ticket === undefined) {
        throw new Error("No ticket")
      }
      const ipcBytes = await collectFlightDataAsIpc(client.doGet(ticket))
      const table = tableFromIPC(ipcBytes)
      return table.numRows
    })
  )

  // ================================
  // Streaming benchmarks
  // ================================
  console.log("\nRunning streaming benchmarks...")

  results.push(
    await benchmark("doGet streaming (count batches)", async () => {
      const info = await client.getFlightInfo(pathDescriptor("test", "integers"))
      const { ticket } = info.endpoint[0]
      if (ticket === undefined) {
        throw new Error("No ticket")
      }
      let batches = 0
      for await (const _data of client.doGet(ticket)) {
        batches++
      }
      return batches
    })
  )

  results.push(
    await benchmark(
      "doGet large streaming (10000 rows, count batches)",
      async () => {
        const info = await client.getFlightInfo(pathDescriptor("test", "large"))
        const { ticket } = info.endpoint[0]
        if (ticket === undefined) {
          throw new Error("No ticket")
        }
        let batches = 0
        for await (const _data of client.doGet(ticket)) {
          batches++
        }
        return batches
      },
      20
    )
  )

  // ================================
  // Concurrent request benchmarks
  // ================================
  console.log("\nRunning concurrent request benchmarks...")

  results.push(
    await benchmark("3 concurrent getFlightInfo", async () => {
      const requests = [
        client.getFlightInfo(pathDescriptor("test", "integers")),
        client.getFlightInfo(pathDescriptor("test", "strings")),
        client.getFlightInfo(pathDescriptor("test", "large"))
      ]
      const infos = await Promise.all(requests)
      return infos.reduce((sum, i) => sum + i.endpoint.length, 0)
    })
  )

  results.push(
    await benchmark("3 concurrent doGet", async () => {
      const infos = await Promise.all([
        client.getFlightInfo(pathDescriptor("test", "integers")),
        client.getFlightInfo(pathDescriptor("test", "strings")),
        client.getFlightInfo(pathDescriptor("test", "nested"))
      ])
      const requests = infos.map(async (info) => {
        const { ticket } = info.endpoint[0]
        if (ticket === undefined) {
          throw new Error("No ticket")
        }
        const ipc = await collectFlightDataAsIpc(client.doGet(ticket))
        return tableFromIPC(ipc)
      })
      const tables = await Promise.all(requests)
      return tables.reduce((sum, t) => sum + t.numRows, 0)
    })
  )

  // ================================
  // High concurrency stress tests
  // ================================
  console.log("\nRunning high-concurrency stress tests...")

  for (const concurrency of [5, 10, 20]) {
    results.push(
      await benchmark(
        `${String(concurrency)} concurrent getFlightInfo`,
        async () => {
          const requests = Array.from({ length: concurrency }, async () =>
            client.getFlightInfo(pathDescriptor("test", "integers"))
          )
          const infos = await Promise.all(requests)
          return infos.reduce((sum, i) => sum + i.endpoint.length, 0)
        },
        20
      )
    )
  }

  results.push(
    await benchmark(
      "Burst: 50 sequential getFlightInfo",
      async () => {
        let total = 0
        for (let i = 0; i < 50; i++) {
          const info = await client.getFlightInfo(pathDescriptor("test", "integers"))
          total += info.endpoint.length
        }
        return total
      },
      5
    )
  )

  // ================================
  // Memory tracking benchmarks
  // ================================
  console.log("\nRunning memory-tracked benchmarks...")

  results.push(
    await benchmark(
      "Large result with memory tracking (10000 rows)",
      async () => {
        const info = await client.getFlightInfo(pathDescriptor("test", "large"))
        const { ticket } = info.endpoint[0]
        if (ticket === undefined) {
          throw new Error("No ticket")
        }
        const ipcBytes = await collectFlightDataAsIpc(client.doGet(ticket))
        const table = tableFromIPC(ipcBytes)
        return { rows: table.numRows, bytes: ipcBytes.length }
      },
      10,
      { trackMemory: true }
    )
  )

  // ================================
  // QPS (Queries Per Second) Sustained Load Tests
  // ================================
  console.log("\n================================")
  console.log("QPS Sustained Load Tests")
  console.log(`Duration: ${String(QPS_DURATION_SEC)}s per concurrency level`)
  console.log(`Concurrency levels: ${QPS_CONCURRENCY_LEVELS.join(", ")}`)
  console.log("================================")

  const qpsResults: QpsResult[] = []

  qpsResults.push(
    ...(await runQpsSweep("getFlightInfo (metadata)", async () => {
      await client.getFlightInfo(pathDescriptor("test", "integers"))
    }))
  )

  qpsResults.push(
    ...(await runQpsSweep("listFlights (iteration)", async () => {
      for await (const _info of client.listFlights()) {
        // consume
      }
    }))
  )

  qpsResults.push(
    ...(await runQpsSweep(
      "doGet 100 rows (full roundtrip)",
      async () => {
        const info = await client.getFlightInfo(pathDescriptor("test", "integers"))
        const { ticket } = info.endpoint[0]
        if (ticket === undefined) {
          throw new Error("No ticket")
        }
        await collectFlightDataAsIpc(client.doGet(ticket))
      },
      [1, 2, 4, 8]
    ))
  )

  console.log("\nQPS CSV Output:")
  console.log(
    "name,concurrency,duration_ms,total_queries,qps,avg_latency_ms,p50_latency_ms,p99_latency_ms,errors"
  )
  for (const r of qpsResults) {
    console.log(
      `"${r.name}",${String(r.concurrency)},${r.durationMs.toFixed(2)},${String(r.totalQueries)},` +
        `${r.qps.toFixed(2)},${r.avgLatencyMs.toFixed(3)},${r.p50LatencyMs.toFixed(3)},` +
        `${r.p99LatencyMs.toFixed(3)},${String(r.errors)}`
    )
  }

  client.close()

  console.log("================================")
  console.log("Benchmark Results Summary")
  console.log("================================\n")

  for (const result of results) {
    formatResult(result)
  }

  const completed = results.filter((r) => r.skipped !== true)
  const skipped = results.filter((r) => r.skipped === true)
  console.log(`\nCompleted: ${String(completed.length)} benchmarks`)
  if (skipped.length > 0) {
    console.log(
      `Skipped: ${String(skipped.length)} benchmarks (${skipped.map((r) => r.name).join(", ")})`
    )
  }

  console.log("\nCSV Output:")
  console.log(
    "name,iterations,total_ms,avg_ms,std_dev,cv_pct,min_ms,max_ms,p50_ms,p95_ms,p99_ms,ops_per_sec,rows_per_sec,bytes_per_sec,memory_delta_mb,skipped"
  )
  for (const r of results) {
    const memMb =
      r.memoryDeltaBytes !== undefined ? (r.memoryDeltaBytes / (1024 * 1024)).toFixed(2) : ""
    console.log(
      `"${r.name}",${String(r.iterations)},${r.totalMs.toFixed(2)},${r.avgMs.toFixed(3)},` +
        `${r.stdDev.toFixed(3)},${r.cv.toFixed(1)},${r.minMs.toFixed(3)},${r.maxMs.toFixed(3)},` +
        `${r.p50Ms.toFixed(3)},${r.p95Ms.toFixed(3)},${r.p99Ms.toFixed(3)},` +
        `${r.opsPerSec?.toFixed(0) ?? ""},${r.rowsPerSec?.toFixed(0) ?? ""},${r.bytesPerSec?.toFixed(0) ?? ""},` +
        `${memMb},${r.skipped === true ? "true" : "false"}`
    )
  }
}

runBenchmarks().catch((err: unknown) => {
  console.error("Benchmark failed:", err)
  process.exit(1)
})
