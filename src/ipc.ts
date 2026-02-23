/**
 * Arrow IPC utilities for Flight data.
 *
 * Provides functions to convert between Flight data format and Arrow IPC format.
 *
 * @packageDocumentation
 */

import type { FlightData } from "./generated/arrow/flight/protocol/Flight.js"

/**
 * IPC continuation token (0xFFFFFFFF).
 * Marks the start of an IPC message in streaming format.
 */
const IPC_CONTINUATION_TOKEN = 0xffffffff

/**
 * Converts FlightData messages to Arrow IPC stream format.
 *
 * The Arrow Flight protocol sends data as FlightData messages where:
 * - `data_header` contains the IPC message flatbuffer (without framing)
 * - `data_body` contains the message body buffers
 *
 * This function re-frames the data into valid Arrow IPC stream format by adding:
 * - Continuation token (0xFFFFFFFF)
 * - Metadata length (int32, little-endian)
 * - Metadata bytes with padding to 8-byte boundary
 * - Body bytes
 *
 * @param flightData - Array of FlightData messages from doGet/doExchange
 * @returns A single Uint8Array containing the Arrow IPC stream
 *
 * @example
 * ```ts
 * import { tableFromIPC } from "apache-arrow"
 * import { createFlightClient, flightDataToIpc, pathDescriptor } from "@qualithm/arrow-flight-js"
 *
 * const client = await createFlightClient({ host: "localhost", port: 8815 })
 * const info = await client.getFlightInfo(pathDescriptor("my", "data"))
 *
 * // Collect all FlightData messages
 * const flightData: FlightData[] = []
 * for await (const data of client.doGet(info.endpoint[0].ticket!)) {
 *   flightData.push(data)
 * }
 *
 * // Convert to IPC format and parse
 * const ipcBytes = flightDataToIpc(flightData)
 * const table = tableFromIPC(ipcBytes)
 * ```
 */
export function flightDataToIpc(flightData: FlightData[]): Uint8Array {
  // Calculate total size needed
  let totalSize = 0

  for (const data of flightData) {
    if (data.dataHeader.length > 0) {
      // 4 bytes continuation + 4 bytes length + header + padding + body
      const headerPadding = calculatePadding(data.dataHeader.length)
      totalSize += 4 + 4 + data.dataHeader.length + headerPadding + data.dataBody.length
    }
  }

  // Allocate buffer
  const result = new Uint8Array(totalSize)
  const view = new DataView(result.buffer)
  let offset = 0

  for (const data of flightData) {
    if (data.dataHeader.length > 0) {
      // Write continuation token (little-endian)
      view.setUint32(offset, IPC_CONTINUATION_TOKEN, true)
      offset += 4

      // Write metadata length (little-endian)
      view.setUint32(offset, data.dataHeader.length, true)
      offset += 4

      // Write metadata (dataHeader)
      result.set(data.dataHeader, offset)
      offset += data.dataHeader.length

      // Add padding to 8-byte boundary
      const padding = calculatePadding(data.dataHeader.length)
      offset += padding // Padding bytes are already 0

      // Write body (dataBody)
      if (data.dataBody.length > 0) {
        result.set(data.dataBody, offset)
        offset += data.dataBody.length
      }
    }
  }

  return result
}

/**
 * Async version of flightDataToIpc that collects from a stream.
 *
 * @param stream - Async iterable of FlightData messages
 * @returns A single Uint8Array containing the Arrow IPC stream
 *
 * @example
 * ```ts
 * import { tableFromIPC } from "apache-arrow"
 * import { createFlightClient, collectFlightDataAsIpc, pathDescriptor } from "@qualithm/arrow-flight-js"
 *
 * const client = await createFlightClient({ host: "localhost", port: 8815 })
 * const info = await client.getFlightInfo(pathDescriptor("my", "data"))
 *
 * // Collect and convert in one step
 * const ipcBytes = await collectFlightDataAsIpc(client.doGet(info.endpoint[0].ticket!))
 * const table = tableFromIPC(ipcBytes)
 * ```
 */
export async function collectFlightDataAsIpc(
  stream: AsyncIterable<FlightData>
): Promise<Uint8Array> {
  const flightData: FlightData[] = []
  for await (const data of stream) {
    flightData.push(data)
  }
  return flightDataToIpc(flightData)
}

/**
 * Calculates padding needed to reach 8-byte alignment.
 */
function calculatePadding(length: number): number {
  const remainder = length % 8
  return remainder === 0 ? 0 : 8 - remainder
}
