/**
 * Browser / gRPC-Web example.
 *
 * Demonstrates connecting to a Flight server from browser environments
 * using gRPC-Web. Requires a gRPC-Web compatible proxy (e.g., Envoy).
 *
 * NOTE: This example is for documentation purposes. It cannot run
 * directly in Node.js/Bun as it requires browser APIs.
 *
 * @example
 * ```bash
 * # This file demonstrates browser usage patterns
 * # To test, integrate this code into a browser application
 * ```
 */

/**
 * Browser-compatible Flight client setup.
 *
 * In browser environments, you need:
 * 1. A gRPC-Web compatible proxy (Envoy is most common)
 * 2. The @grpc/grpc-web package instead of @grpc/grpc-js
 * 3. Your Flight server configured to work with the proxy
 */

// =============================================================================
// ARCHITECTURE OVERVIEW
// =============================================================================
//
// Browser ──────> Envoy Proxy ──────> Flight Server
//   │               │                    │
//   │ gRPC-Web      │ gRPC (HTTP/2)      │
//   │ (HTTP/1.1     │                    │
//   │  or HTTP/2)   │                    │
//
// Why is a proxy needed?
// - Browsers cannot make raw HTTP/2 gRPC calls
// - gRPC-Web is a subset of gRPC that works over HTTP/1.1
// - Envoy translates between gRPC-Web and native gRPC
//
// =============================================================================

// =============================================================================
// ENVOY CONFIGURATION
// =============================================================================
//
// Example Envoy configuration for Flight gRPC-Web proxy:
//
// ```yaml
// static_resources:
//   listeners:
//     - name: listener_0
//       address:
//         socket_address:
//           address: 0.0.0.0
//           port_value: 8080
//       filter_chains:
//         - filters:
//             - name: envoy.filters.network.http_connection_manager
//               typed_config:
//                 "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
//                 stat_prefix: ingress_http
//                 codec_type: AUTO
//                 route_config:
//                   name: local_route
//                   virtual_hosts:
//                     - name: local_service
//                       domains: ["*"]
//                       routes:
//                         - match:
//                             prefix: "/"
//                           route:
//                             cluster: flight_service
//                       cors:
//                         allow_origin_string_match:
//                           - prefix: "*"
//                         allow_methods: GET, PUT, DELETE, POST, OPTIONS
//                         allow_headers: keep-alive,user-agent,cache-control,content-type,content-transfer-encoding,x-accept-content-transfer-encoding,x-accept-response-streaming,x-user-agent,x-grpc-web,grpc-timeout,authorization
//                         expose_headers: grpc-status,grpc-message,authorization
//                         max_age: "1728000"
//                 http_filters:
//                   - name: envoy.filters.http.grpc_web
//                     typed_config:
//                       "@type": type.googleapis.com/envoy.extensions.filters.http.grpc_web.v3.GrpcWeb
//                   - name: envoy.filters.http.cors
//                     typed_config:
//                       "@type": type.googleapis.com/envoy.extensions.filters.http.cors.v3.Cors
//                   - name: envoy.filters.http.router
//                     typed_config:
//                       "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
//   clusters:
//     - name: flight_service
//       connect_timeout: 0.25s
//       type: LOGICAL_DNS
//       lb_policy: ROUND_ROBIN
//       http2_protocol_options: {}
//       load_assignment:
//         cluster_name: flight_service
//         endpoints:
//           - lb_endpoints:
//               - endpoint:
//                   address:
//                     socket_address:
//                       address: flight-server
//                       port_value: 8815
// ```
//
// =============================================================================

// =============================================================================
// BROWSER CLIENT EXAMPLE
// =============================================================================
//
// In your browser application (React, Vue, vanilla JS, etc.):
//
// ```typescript
// // Install browser-compatible packages:
// // npm install @grpc/grpc-web apache-arrow
//
// import { grpc } from "@grpc/grpc-web"
//
// // The Flight service client generated for grpc-web
// // (you'll need to generate this from Flight.proto with grpc-web plugin)
// import { FlightServiceClient } from "./generated/FlightServiceClientPb"
//
// // Create the client pointing to the Envoy proxy
// const client = new FlightServiceClient("https://your-envoy-proxy.example.com:8080")
//
// // List flights
// async function listFlights() {
//   const request = new Criteria()
//   request.setExpression(new Uint8Array())
//
//   const stream = client.listFlights(request, {})
//
//   stream.on("data", (flightInfo) => {
//     console.log("Flight:", flightInfo.toObject())
//   })
//
//   stream.on("error", (err) => {
//     console.error("Error:", err)
//   })
//
//   stream.on("end", () => {
//     console.log("Done listing flights")
//   })
// }
//
// // Get flight info
// async function getFlightInfo(path: string[]) {
//   const descriptor = new FlightDescriptor()
//   descriptor.setType(FlightDescriptor.DescriptorType.PATH)
//   descriptor.setPathList(path)
//
//   return new Promise((resolve, reject) => {
//     client.getFlightInfo(descriptor, {}, (err, response) => {
//       if (err) reject(err)
//       else resolve(response)
//     })
//   })
// }
//
// // Retrieve data (doGet)
// async function getData(ticket: Ticket) {
//   const chunks: Uint8Array[] = []
//   const stream = client.doGet(ticket, {})
//
//   stream.on("data", (flightData) => {
//     const body = flightData.getDataBody_asU8()
//     if (body.length > 0) {
//       chunks.push(body)
//     }
//   })
//
//   return new Promise((resolve, reject) => {
//     stream.on("end", () => {
//       // Parse with Apache Arrow
//       const combined = concatenateUint8Arrays(chunks)
//       const table = tableFromIPC(combined)
//       resolve(table)
//     })
//     stream.on("error", reject)
//   })
// }
// ```
//
// =============================================================================

// =============================================================================
// REACT EXAMPLE
// =============================================================================
//
// ```tsx
// import { useState, useEffect } from "react"
// import { FlightServiceClient } from "./generated/FlightServiceClientPb"
// import { Criteria, FlightDescriptor } from "./generated/Flight_pb"
// import { tableFromIPC } from "apache-arrow"
//
// const flightClient = new FlightServiceClient(
//   import.meta.env.VITE_FLIGHT_PROXY_URL
// )
//
// function FlightDataViewer({ path }: { path: string[] }) {
//   const [data, setData] = useState<any[]>([])
//   const [loading, setLoading] = useState(true)
//   const [error, setError] = useState<Error | null>(null)
//
//   useEffect(() => {
//     async function fetchData() {
//       try {
//         setLoading(true)
//
//         // Get flight info
//         const descriptor = new FlightDescriptor()
//         descriptor.setType(FlightDescriptor.DescriptorType.PATH)
//         descriptor.setPathList(path)
//
//         const info = await new Promise((resolve, reject) => {
//           flightClient.getFlightInfo(descriptor, {}, (err, res) => {
//             if (err) reject(err)
//             else resolve(res)
//           })
//         })
//
//         // Get data from first endpoint
//         const ticket = info.getEndpointList()[0].getTicket()
//         const chunks: Uint8Array[] = []
//
//         const stream = flightClient.doGet(ticket, {})
//
//         stream.on("data", (flightData) => {
//           const body = flightData.getDataBody_asU8()
//           if (body.length > 0) chunks.push(body)
//         })
//
//         await new Promise((resolve, reject) => {
//           stream.on("end", resolve)
//           stream.on("error", reject)
//         })
//
//         // Parse Arrow data
//         const combined = new Uint8Array(
//           chunks.reduce((acc, c) => acc + c.length, 0)
//         )
//         let offset = 0
//         for (const chunk of chunks) {
//           combined.set(chunk, offset)
//           offset += chunk.length
//         }
//
//         const table = tableFromIPC(combined)
//         setData(table.toArray().map(row => row.toJSON()))
//
//       } catch (err) {
//         setError(err as Error)
//       } finally {
//         setLoading(false)
//       }
//     }
//
//     fetchData()
//   }, [path])
//
//   if (loading) return <div>Loading...</div>
//   if (error) return <div>Error: {error.message}</div>
//
//   return (
//     <table>
//       <thead>
//         <tr>
//           {Object.keys(data[0] || {}).map(key => (
//             <th key={key}>{key}</th>
//           ))}
//         </tr>
//       </thead>
//       <tbody>
//         {data.map((row, i) => (
//           <tr key={i}>
//             {Object.values(row).map((val, j) => (
//               <td key={j}>{String(val)}</td>
//             ))}
//           </tr>
//         ))}
//       </tbody>
//     </table>
//   )
// }
//
// export default FlightDataViewer
// ```
//
// =============================================================================

// =============================================================================
// AUTHENTICATION IN BROWSER
// =============================================================================
//
// ```typescript
// // Add authentication headers to gRPC-Web calls
//
// const metadata = {
//   "Authorization": `Bearer ${accessToken}`,
//   "X-Request-ID": crypto.randomUUID()
// }
//
// // Pass metadata to each call
// client.getFlightInfo(descriptor, metadata, (err, response) => {
//   // ...
// })
//
// // For streaming calls
// const stream = client.doGet(ticket, metadata)
// ```
//
// =============================================================================

// =============================================================================
// LIMITATIONS
// =============================================================================
//
// gRPC-Web has some limitations compared to native gRPC:
//
// 1. **No bidirectional streaming** - doExchange and doPut have limited support
//    - Server streaming (doGet, listFlights) works
//    - Client streaming requires server-side buffering
//    - Full duplex is not possible
//
// 2. **Performance overhead** - Base64 encoding in text mode adds ~33% size
//    - Use binary mode when possible (application/grpc-web+proto)
//
// 3. **Proxy required** - Cannot connect directly to Flight servers
//    - Adds deployment complexity
//    - Envoy is the most common choice
//
// 4. **CORS configuration** - Must be properly configured on the proxy
//    - Can be complex in enterprise environments
//
// =============================================================================

// This file is documentation-only and does not export runnable code
export {}
