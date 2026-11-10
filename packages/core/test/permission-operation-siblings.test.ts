import { expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Deferred, Effect, Fiber, Stream } from "effect"
import { Bus } from "@opencode/core/bus"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { Permission } from "@opencode/core/permission"
import { it, fixture } from "./fixture/permission-operation"
import { executeTool, toolIdentity } from "./lib/tool"

for (const change of ["allow", "deny", "move", "reload", "subtree-deny"] as const)
  it.live(`earlier sibling authority stays current across later sibling ask (${change})`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const locations = yield* LocationServiceMap.Service
      const first = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const ancestor =
        change === "subtree-deny"
          ? yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
          : undefined
      const second = yield* input.actor("ask", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const payload = "sibling-protected-payload-84217"
      yield* Effect.promise(() => fs.writeFile(path.join(input.directories[1], "src/main.go"), payload))
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID !== target.id) return
          event.requireAuthority({ sessionID: ancestor?.id ?? first.id })
          event.requireAuthority({ sessionID: second.id })
        }),
      )
      if (ancestor)
        yield* input.callerServices.hooks.register("permission", "experimental.operation", (event) =>
          Effect.sync(() => {
            if (event.sessionID === ancestor.id) event.requireAuthority({ sessionID: first.id })
          }),
        )
      const asked = yield* Deferred.make<Permission.Request>()
      const bus = yield* Bus.Service
      yield* bus.subscribe(Permission.Event.Asked).pipe(
        Stream.runForEach((event) =>
          event.data.sessionID === second.id ? Deferred.succeed(asked, event.data).pipe(Effect.asVoid) : Effect.void,
        ),
        Effect.forkScoped({ startImmediately: true }),
      )
      const fiber = yield* executeTool(input.targetServices.tools, {
        sessionID: target.id,
        ...toolIdentity,
        call: { type: "tool-call", id: "sibling-read", name: "read", input: { path: "src/main.go" } },
      }).pipe(Effect.forkScoped({ startImmediately: true }))
      const request = yield* Deferred.await(asked)
      expect(request.sessionID).toBe(second.id)
      if (change === "deny" || change === "subtree-deny")
        yield* input.sessions.setPermissions({
          sessionID: first.id,
          permissions: [
            { action: "*", resource: "*", effect: "allow" },
            { action: "read", resource: "*", effect: "deny" },
          ],
        })
      if (change === "move") {
        yield* input.sessions.move({ sessionID: first.id, directory: input.targetRef.directory })
        yield* input.sessions.wait(first.id)
      }
      if (change === "reload") yield* locations.invalidate(input.callerRef)
      yield* input.targetServices.permission.reply({ requestID: request.id, reply: "once" })
      const result = yield* Fiber.join(fiber)
      expect(result.status).toBe(change === "allow" ? "completed" : "error")
      expect(JSON.stringify(result).includes(payload)).toBe(change === "allow")
      expect(yield* input.targetServices.permission.list()).toHaveLength(0)
      expect(yield* input.callerServices.permission.list()).toHaveLength(0)
    }),
  )
