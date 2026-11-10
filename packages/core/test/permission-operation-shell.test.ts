import { expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Deferred, Effect, Exit, Fiber, Stream } from "effect"
import type { ShellCreateBefore } from "@opencode/plugin/effect/shell"
import { Bus } from "@opencode/core/bus"
import { Permission } from "@opencode/core/permission"
import { it, fixture } from "./fixture/permission-operation"
import { executeTool, toolIdentity } from "./lib/tool"

for (const mode of ["allow", "caller-deny", "target-deny", "boundary-deny", "changed-command", "changed-cwd"] as const)
  it.live(`delegated shell authorizes final native scan (${mode})`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const marker = path.join(input.directories[1], "shell-marker.txt")
      const allowed = mode === "allow"
      yield* input.sessions.setPermissions({
        sessionID: mode === "target-deny" ? target.id : caller.id,
        permissions: [
          { action: "*", resource: "*", effect: "allow" },
          ...(mode === "caller-deny" || mode === "target-deny" || mode === "changed-command"
            ? [
                {
                  action: "shell",
                  resource: mode === "changed-command" ? "printf changed*" : "*",
                  effect: "deny" as const,
                },
              ]
            : []),
          ...(mode === "boundary-deny" || mode === "changed-cwd"
            ? [{ action: "external_directory", resource: "*", effect: "deny" as const }]
            : []),
        ],
      })
      if (mode === "changed-command" || mode === "changed-cwd")
        yield* input.targetServices.hooks.register("shell", "create.before", (event) =>
          Effect.sync(() => {
            if (mode === "changed-command") event.command = `printf changed > '${marker}'`
            else event.cwd = input.directories[1]
          }),
        )
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      const result = yield* executeTool(input.targetServices.tools, {
        sessionID: target.id,
        ...toolIdentity,
        call: {
          type: "tool-call",
          id: "shell-leaf",
          name: "shell",
          input: {
            command:
              mode === "changed-cwd" ? "printf inert" : `printf first > '${marker}' && printf second >> '${marker}'`,
            workdir: mode === "changed-cwd" ? input.directories[0] : input.directories[1],
          },
        },
      })
      expect(result.status).toBe(allowed ? "completed" : "error")
      expect(yield* Effect.promise(() => Bun.file(marker).exists())).toBe(allowed)
      if (allowed) expect(yield* Effect.promise(() => fs.readFile(marker, "utf8"))).toBe("firstsecond")
    }),
  )

for (const response of ["once", "cancel"] as const)
  it.live(`delegated shell suspends spawn until native caller ${response}`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      yield* input.sessions.setPermissions({
        sessionID: caller.id,
        permissions: [
          { action: "*", resource: "*", effect: "allow" },
          { action: "shell", resource: "*", effect: "ask" },
        ],
      })
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      const bus = yield* Bus.Service
      const asked = yield* Deferred.make<Permission.Request>()
      yield* bus.subscribe(Permission.Event.Asked).pipe(
        Stream.runForEach((event) => Deferred.succeed(asked, event.data)),
        Effect.forkScoped({ startImmediately: true }),
      )
      const marker = path.join(input.directories[1], "pending-marker.txt")
      const fiber = yield* executeTool(input.targetServices.tools, {
        sessionID: target.id,
        ...toolIdentity,
        call: {
          type: "tool-call",
          id: "shell-pending",
          name: "shell",
          input: { command: `printf ready > '${marker}'` },
        },
      }).pipe(Effect.forkScoped({ startImmediately: true }))
      const next = yield* Effect.raceFirst(
        Deferred.await(asked).pipe(Effect.map((request) => ({ kind: "ask" as const, request }))),
        Fiber.await(fiber).pipe(Effect.as({ kind: "ended" as const })),
      )
      expect(next.kind).toBe("ask")
      if (next.kind !== "ask") return
      expect(next.request.sessionID).toBe(caller.id)
      expect(yield* Effect.promise(() => Bun.file(marker).exists())).toBe(false)
      if (response === "cancel") yield* Fiber.interrupt(fiber)
      else {
        yield* input.callerServices.permission.reply({ requestID: next.request.id, reply: "once" })
        expect((yield* Fiber.join(fiber)).status).toBe("completed")
      }
      expect(yield* Effect.promise(() => Bun.file(marker).exists())).toBe(response === "once")
      expect(yield* input.callerServices.permission.list()).toHaveLength(0)
    }),
  )

it.live("retained shell hook cannot mutate invocation after native authority snapshot", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    let retained: ShellCreateBefore | undefined
    yield* input.targetServices.hooks.register("shell", "create.before", (event) =>
      Effect.sync(() => {
        retained = event
      }),
    )
    const marker = path.join(input.directories[1], "mutated-marker.txt")
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    yield* input.callerServices.hooks.register("permission", "evaluate", (event) =>
      Effect.sync(() => {
        if (event.action === "shell" && retained) Object.assign(retained, { command: `printf forbidden > '${marker}'` })
      }),
    )
    const exit = yield* executeTool(input.targetServices.tools, {
      sessionID: target.id,
      ...toolIdentity,
      call: { type: "tool-call", id: "shell-mutation", name: "shell", input: { command: "printf inert" } },
    }).pipe(Effect.exit)
    expect(Exit.isFailure(exit) || exit.value.status === "error").toBe(true)
    expect(yield* Effect.promise(() => Bun.file(marker).exists())).toBe(false)
  }),
)
