/**
 * Integration tests for Flight actions: doAction, listActions.
 *
 * Requires a running Arrow Flight server.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test"

import { type ActionType, createFlightClient, type FlightClient, type Result } from "../../index"
import { config } from "./config"

describe("Actions Integration", () => {
  let client: FlightClient

  beforeAll(async () => {
    client = await createFlightClient({
      host: config.host,
      port: config.port,
      tls: config.tls,
      auth: {
        type: "basic",
        username: config.credentials.admin.username,
        password: config.credentials.admin.password
      }
    })
    await client.handshake()
  })

  afterAll(() => {
    client.close()
  })

  describe("listActions", () => {
    it("lists available actions", async () => {
      const actions: ActionType[] = []

      for await (const action of client.listActions()) {
        actions.push(action)
      }

      // Server should have at least healthcheck, echo, error actions
      expect(actions.length).toBeGreaterThan(0)

      const actionTypes = actions.map((a) => a.type)
      expect(actionTypes).toContain("healthcheck")
      expect(actionTypes).toContain("echo")
      expect(actionTypes).toContain("error")
    })

    it("returns action descriptions", async () => {
      for await (const action of client.listActions()) {
        expect(action.type).toBeDefined()
        expect(action.type.length).toBeGreaterThan(0)
        // Description may be empty but should be defined
        expect(action.description).toBeDefined()
      }
    })
  })

  describe("doAction", () => {
    it("executes healthcheck action", async () => {
      const results: Result[] = []

      for await (const result of client.doAction({
        type: "healthcheck",
        body: Buffer.alloc(0)
      })) {
        results.push(result)
      }

      expect(results.length).toBe(1)

      // Parse the JSON response
      const body = JSON.parse(results[0].body.toString("utf8")) as { status: string }
      expect(body.status).toBe("ok")
    })

    it("executes echo action", async () => {
      const testPayload = Buffer.from("Hello, Flight!")
      const results: Result[] = []

      for await (const result of client.doAction({
        type: "echo",
        body: testPayload
      })) {
        results.push(result)
      }

      expect(results.length).toBe(1)
      // Echo should return the same bytes
      expect(Buffer.from(results[0].body)).toEqual(testPayload)
    })

    it("error action returns INTERNAL error", async () => {
      try {
        for await (const _ of client.doAction({
          type: "error",
          body: Buffer.alloc(0)
        })) {
          // Should not reach here
        }
        expect.unreachable("Expected INTERNAL error")
      } catch (error) {
        expect((error as { code: string }).code).toBe("INTERNAL")
      }
    })

    it("returns NOT_FOUND for unknown action", async () => {
      try {
        for await (const _ of client.doAction({
          type: "unknown-action",
          body: Buffer.alloc(0)
        })) {
          // Should not reach here
        }
        expect.unreachable("Expected NOT_FOUND error")
      } catch (error) {
        expect((error as { code: string }).code).toBe("NOT_FOUND")
      }
    })
  })
})
