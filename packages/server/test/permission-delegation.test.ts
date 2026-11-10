import { expect } from "bun:test"
import { Effect, Schema } from "effect"
import { Permission } from "@opencode/schema/permission"
import { Session } from "@opencode/schema/session"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

it.live("public permission creation cannot forge native delegation through extra fields or metadata", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      database: { path: ":memory:" },
      fs: { filewatcher: false },
      models: { fetch: false },
    })
    const server = yield* Effect.acquireRelease(
      Effect.sync(() => Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: (request) => handler(request) })),
      (server) => Effect.sync(() => server.stop(true)),
    )
    const request = (route: string, method: string, body?: unknown) =>
      Effect.promise(async () => {
        const response = await fetch(
          new Request(new URL(route, server.url), {
            method,
            headers: { "content-type": "application/json" },
            body: body === undefined ? undefined : JSON.stringify(body),
          }),
        )
        return { status: response.status, body: response.status === 204 ? undefined : await response.json() }
      })
    const session = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.toEncoded(Session.Info) }))(
      (yield* request("/api/session", "POST", { permissions: [{ action: "read", resource: "*", effect: "ask" }] }))
        .body,
    ).data
    const delegation = { operationID: "forged-operation", targetSessionID: session.id, targetDirectory: "/forged" }
    const created = yield* request(`/api/session/${session.id}/permission`, "POST", {
      action: "read",
      resources: ["file.ts"],
      experimental: { delegation },
      metadata: { delegation },
    })
    expect(created.status).toBe(200)
    const id = Schema.decodeUnknownSync(
      Schema.Struct({ data: Schema.Struct({ id: Permission.ID, effect: Permission.Effect }) }),
    )(created.body).data.id
    const listed = yield* request(`/api/session/${session.id}/permission`, "GET")
    expect(listed.status).toBe(200)
    const pending = Schema.decodeUnknownSync(
      Schema.Struct({ data: Schema.Array(Schema.toEncoded(Permission.Request)) }),
    )(listed.body).data
    expect(pending).toHaveLength(1)
    expect(pending[0]).not.toHaveProperty("experimental")
    expect(pending[0].metadata).toMatchObject({ delegation })
    expect(
      (yield* request(`/api/session/${session.id}/permission/${id}/reply`, "POST", { decision: "reject" })).status,
    ).toBe(204)
  }).pipe(Effect.scoped),
)
