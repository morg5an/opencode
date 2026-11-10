import { expect } from "bun:test"
import path from "path"
import { Cause, Deferred, Effect, Exit, Fiber, Stream } from "effect"
import type { PermissionOperationEvaluation } from "@opencode/plugin/permission"
import { Bus } from "@opencode/core/bus"
import { Session } from "@opencode/core/session"
import { Permission } from "@opencode/core/permission"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { Agent } from "@opencode/core/agent"
import { AbsolutePath } from "@opencode/core/schema"
import { Tool } from "@opencode/core/tool"
import { toolIdentity } from "./lib/tool"
import { it, fixture } from "./fixture/permission-operation"

it.live("concurrent same-call episodes keep caller once approvals and cancellation separate", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("ask", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    const intent = {
      kind: "file-path" as const,
      action: "read" as const,
      targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" as const }],
    }
    const invocation = { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("shared-call") }
    const episodes = [
      yield* input.targetServices.operations.begin(intent, invocation),
      yield* input.targetServices.operations.begin(intent, invocation),
    ]
    const asks = [yield* Deferred.make<Permission.Request>(), yield* Deferred.make<Permission.Request>()]
    const bus = yield* Bus.Service
    yield* bus.subscribe(Permission.Event.Asked).pipe(
      Stream.runForEach((event) => {
        const index = episodes.findIndex((episode) => episode.id === event.data.experimental?.delegation?.operationID)
        return index < 0 ? Effect.void : Deferred.succeed(asks[index], event.data).pipe(Effect.asVoid)
      }),
      Effect.forkScoped({ startImmediately: true }),
    )
    let effects = 0
    const invoke = (index: number) =>
      Effect.gen(function* () {
        yield* input.targetServices.operations.authorizeEffect(episodes[index])
        effects++
      }).pipe(Effect.forkScoped({ startImmediately: true }))
    const first = yield* invoke(0)
    const second = yield* invoke(1)
    const requests = [yield* Deferred.await(asks[0]), yield* Deferred.await(asks[1])]
    expect(requests[0].id).not.toBe(requests[1].id)
    expect(effects).toBe(0)
    yield* input.callerServices.permission.reply({ requestID: requests[0].id, reply: "once" })
    yield* Fiber.join(first)
    expect(effects).toBe(1)
    expect(yield* input.callerServices.permission.list()).toHaveLength(1)
    yield* Fiber.interrupt(second)
    expect(effects).toBe(1)
    expect(yield* input.callerServices.permission.list()).toHaveLength(0)
  }),
)

it.live("invalidating caller location rejects pending native authority and removes waiter", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const locations = yield* LocationServiceMap.Service
    const caller = yield* input.actor("ask", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    const asked = yield* Deferred.make<Permission.Request>()
    const bus = yield* Bus.Service
    yield* bus.subscribe(Permission.Event.Asked).pipe(
      Stream.runForEach((event) => Deferred.succeed(asked, event.data)),
      Effect.forkScoped({ startImmediately: true }),
    )
    const episode = yield* input.targetServices.operations.begin(
      {
        kind: "file-path",
        action: "read",
        targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
      },
      { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("reload-call") },
    )
    let effects = 0
    const fiber = yield* Effect.gen(function* () {
      yield* input.targetServices.operations.authorizeEffect(episode)
      effects++
    }).pipe(Effect.forkScoped({ startImmediately: true }))
    yield* Deferred.await(asked)
    yield* locations.invalidate(input.callerRef)
    expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true)
    expect(effects).toBe(0)
    expect(yield* input.callerServices.permission.list()).toHaveLength(0)
  }),
)

it.live("composes actual caller and target policies in distinct native projects", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    let effects = 0
    for (const row of [
      { caller: "deny", target: "allow", allowed: false },
      { caller: "allow", target: "deny", allowed: false },
      { caller: "allow", target: "allow", allowed: true },
      { caller: "deny", target: "deny", allowed: false },
    ] as const) {
      const caller = yield* input.actor(row.caller, input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor(row.target, input.targetRef).pipe(Effect.provideContext(input.targetContext))
      expect(caller.projectID).not.toBe(target.projectID)
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      const episode = yield* input.targetServices.operations.begin(
        {
          kind: "file-path",
          action: "read",
          targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
        },
        { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
      )
      const result = yield* Effect.gen(function* () {
        yield* input.targetServices.operations.authorizeBoundary(episode)
        yield* input.targetServices.operations.authorizeEffect(episode)
        effects++
      }).pipe(Effect.exit)
      expect(Exit.isSuccess(result)).toBe(row.allowed)
      expect(yield* input.targetServices.permission.list()).toHaveLength(0)
    }
    expect(effects).toBe(1)
  }),
)

it.live("raw permission methods cannot skip a required caller without native intent", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    const request = { sessionID: target.id, action: "read", resources: ["src/main.go"] }
    expect((yield* input.targetServices.permission.ask(request)).effect).toBe("deny")
    expect(yield* input.targetServices.permission.allows(request)).toBe(false)
    expect(Exit.isFailure(yield* input.targetServices.permission.assert(request).pipe(Effect.exit))).toBe(true)
    expect(Exit.isFailure(yield* input.targetServices.permission.assertNotDenied(request).pipe(Effect.exit))).toBe(true)
    expect(yield* input.targetServices.permission.list()).toHaveLength(0)
  }),
)

for (const reply of ["once", "always", "reject", "cancel", "tighten", "target-reject"] as const)
  it.live(`caller and target native asks both suspend effect (${reply})`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("ask", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("ask", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      expect(caller.projectID).not.toBe(target.projectID)
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      const bus = yield* Bus.Service
      const firstAsk = yield* Deferred.make<Permission.Request>()
      const targetAsk = yield* Deferred.make<Permission.Request>()
      yield* bus.subscribe(Permission.Event.Asked).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(firstAsk, event.data)
            if (event.data.sessionID === target.id) yield* Deferred.succeed(targetAsk, event.data)
          }),
        ),
        Effect.forkScoped({ startImmediately: true }),
      )
      const episode = yield* input.targetServices.operations.begin(
        {
          kind: "file-path",
          action: "read",
          targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
        },
        { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
      )
      let effects = 0
      const fiber = yield* Effect.gen(function* () {
        yield* input.targetServices.operations.authorizeBoundary(episode)
        yield* input.targetServices.operations.authorizeEffect(episode)
        effects++
      }).pipe(Effect.forkScoped({ startImmediately: true }))
      const first = yield* Effect.raceFirst(
        Deferred.await(firstAsk).pipe(Effect.map((request) => ({ kind: "ask" as const, request }))),
        Fiber.await(fiber).pipe(Effect.map((exit) => ({ kind: "ended" as const, exit }))),
      )
      expect(
        first.kind,
        first.kind === "ended" && Exit.isFailure(first.exit) ? Cause.pretty(first.exit.cause) : "Expected native ask",
      ).toBe("ask")
      if (first.kind !== "ask") return
      expect(first.request.sessionID).toBe(caller.id)
      expect(first.request.source).toBeUndefined()
      expect(first.request).toMatchObject({
        experimental: {
          delegation: {
            operationID: episode.id,
            targetSessionID: target.id,
            targetDirectory: input.targetRef.directory,
            source: { type: "tool", messageID: toolIdentity.messageID, id: "same-call" },
          },
        },
      })
      expect(first.request.resources).toEqual([path.join(input.directories[1], "src/main.go")])
      expect(yield* input.targetServices.permission.list()).toHaveLength(0)
      expect(effects).toBe(0)
      if (reply === "cancel") {
        yield* Fiber.interrupt(fiber)
        expect(effects).toBe(0)
        expect(yield* input.callerServices.permission.list()).toHaveLength(0)
        expect(yield* input.targetServices.permission.list()).toHaveLength(0)
        return
      }
      if (reply === "reject") {
        yield* input.callerServices.permission.reply({ requestID: first.request.id, reply: "reject" })
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true)
        expect(effects).toBe(0)
        expect(yield* input.callerServices.permission.list()).toHaveLength(0)
        expect(yield* input.targetServices.permission.list()).toHaveLength(0)
        return
      }
      yield* input.callerServices.permission.reply({
        requestID: first.request.id,
        reply: reply === "tighten" ? "once" : reply === "target-reject" ? "always" : reply,
      })
      if (reply === "always" || reply === "target-reject") {
        const grants = yield* input.callerServices.saved.list({ projectID: caller.projectID })
        expect(grants).toHaveLength(1)
        expect(grants[0]).toMatchObject({ projectID: caller.projectID, action: "read", resource: "*" })
        expect(yield* input.callerServices.saved.list({ projectID: target.projectID })).toHaveLength(0)
      }
      const second = yield* Deferred.await(targetAsk)
      expect(second.sessionID).toBe(target.id)
      expect(second).not.toHaveProperty("experimental")
      expect(second.resources).toEqual(["src/main.go"])
      expect(effects).toBe(0)
      if (reply === "tighten")
        yield* input.sessions.setPermissions({
          sessionID: caller.id,
          permissions: [
            { action: "*", resource: "*", effect: "allow" },
            { action: "read", resource: "*", effect: "deny" },
          ],
        })
      if (reply === "target-reject") {
        yield* input.targetServices.permission.reply({ requestID: second.id, reply: "reject" })
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true)
        expect(effects).toBe(0)
        expect(yield* input.callerServices.saved.list({ projectID: caller.projectID })).toHaveLength(1)
        expect(yield* input.callerServices.saved.list({ projectID: target.projectID })).toHaveLength(0)
        return
      }
      yield* input.targetServices.permission.reply({ requestID: second.id, reply: "once" })
      if (reply === "tighten") {
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true)
        expect(effects).toBe(0)
        expect(yield* input.callerServices.permission.list()).toHaveLength(0)
        expect(yield* input.targetServices.permission.list()).toHaveLength(0)
        return
      }
      yield* Fiber.join(fiber)
      expect(effects).toBe(1)
      expect(yield* input.callerServices.permission.list()).toHaveLength(0)
      expect(yield* input.targetServices.permission.list()).toHaveLength(0)
      expect(yield* input.sessions.context(caller.id)).toHaveLength(0)
      expect(yield* input.sessions.context(target.id)).toHaveLength(0)
    }),
  )

it.live("same agent name resolves caller rules in caller location", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    yield* input.callerServices.agents.transform((editor) =>
      editor.update(Agent.ID.make("build"), (agent) => {
        agent.permissions = [
          { action: "*", resource: "*", effect: "allow" },
          { action: "read", resource: "*", effect: "deny" },
        ]
      }),
    )
    const caller = yield* input.sessions.create({ location: input.callerRef, agent: Agent.ID.make("build") })
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    const episode = yield* input.targetServices.operations.begin(
      {
        kind: "file-path",
        action: "read",
        targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
      },
      { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
    )
    const result = yield* Effect.gen(function* () {
      yield* input.targetServices.operations.authorizeBoundary(episode)
      yield* input.targetServices.operations.authorizeEffect(episode)
    }).pipe(Effect.exit)
    expect(Exit.isFailure(result)).toBe(true)
  }),
)

it.live("authority cycles fail closed without creating approvals", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: target.id })
      }),
    )
    const episode = yield* input.targetServices.operations.begin(
      {
        kind: "file-path",
        action: "read",
        targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
      },
      { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
    )
    const failure = yield* input.targetServices.operations.authorizeBoundary(episode).pipe(Effect.flip)
    expect(failure).toBeInstanceOf(Permission.BlockedError)
    expect(failure.message).toBe("Authority cycle detected")
    expect(yield* input.targetServices.permission.list()).toHaveLength(0)
  }),
)

it.live("moving caller between phases invalidates original authority instead of adopting target scope", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    const episode = yield* input.targetServices.operations.begin(
      {
        kind: "file-path",
        action: "read",
        targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
      },
      { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
    )
    yield* input.targetServices.operations.authorizeBoundary(episode)
    yield* input.sessions.move({ sessionID: caller.id, directory: input.targetRef.directory })
    yield* input.sessions.wait(caller.id)
    expect((yield* input.sessions.get(caller.id)).location.directory).toBe(input.targetRef.directory)
    expect(Exit.isFailure(yield* input.targetServices.operations.authorizeEffect(episode).pipe(Effect.exit))).toBe(true)
  }),
)

it.live("added authority cannot disappear between boundary and effect phases", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    let required = true
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id && required) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    const episode = yield* input.targetServices.operations.begin(
      {
        kind: "file-path",
        action: "read",
        targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
      },
      { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
    )
    yield* input.targetServices.operations.authorizeBoundary(episode)
    required = false
    yield* input.sessions.setPermissions({
      sessionID: caller.id,
      permissions: [
        { action: "*", resource: "*", effect: "allow" },
        { action: "read", resource: "*", effect: "deny" },
      ],
    })
    expect(Exit.isFailure(yield* input.targetServices.operations.authorizeEffect(episode).pipe(Effect.exit))).toBe(true)
  }),
)

it.live("authority collector expires after its native hook phase", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    let retained: PermissionOperationEvaluation["requireAuthority"] | undefined
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) {
          retained = event.requireAuthority
          event.requireAuthority({ sessionID: caller.id })
        }
      }),
    )
    expect(
      (yield* input.targetServices.permission.ask({ sessionID: target.id, action: "read", resources: ["src/main.go"] }))
        .effect,
    ).toBe("deny")
    const invoke = retained
    if (!invoke) throw new Error("Expected authority collector")
    expect(() => invoke({ sessionID: caller.id })).toThrow("Expired authority collector")
  }),
)

it.live("collector cannot be replaced to silence a later native authority requirement", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("deny", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        Object.assign(event, { requireAuthority: () => {} })
      }),
    )
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    const result = yield* input.targetServices.permission
      .ask({ sessionID: target.id, action: "read", resources: ["src/main.go"] })
      .pipe(Effect.exit)
    expect(Exit.isFailure(result) || result.value.effect === "deny").toBe(true)
  }),
)

for (const limit of ["depth", "principals"] as const)
  it.live(`bounds delegated native authority ${limit}`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const callers = []
      for (let index = 0; index < (limit === "depth" ? 8 : 16); index++)
        callers.push(yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext)))
      const links = new Map<Session.ID, readonly Session.ID[]>()
      links.set(target.id, limit === "depth" ? [callers[0].id] : callers.map((caller) => caller.id))
      if (limit === "depth")
        for (const [index, caller] of callers.entries())
          if (callers[index + 1]) links.set(caller.id, [callers[index + 1].id])
      const require = (event: PermissionOperationEvaluation) =>
        Effect.sync(() => {
          for (const sessionID of links.get(event.sessionID) ?? []) event.requireAuthority({ sessionID })
        })
      yield* input.targetServices.hooks.register("permission", "experimental.operation", require)
      yield* input.callerServices.hooks.register("permission", "experimental.operation", require)
      const episode = yield* input.targetServices.operations.begin(
        {
          kind: "file-path",
          action: "read",
          targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
        },
        { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
      )
      const failure = yield* input.targetServices.operations.authorizeBoundary(episode).pipe(Effect.flip)
      expect(failure).toBeInstanceOf(Permission.BlockedError)
      expect(failure.message).toBe("Native authority limit exceeded")
      expect(yield* input.targetServices.permission.list()).toHaveLength(0)
      expect(yield* input.callerServices.permission.list()).toHaveLength(0)
    }),
  )

it.live("additional authority input cannot override agent or supplied resources", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id)
          event.requireAuthority(Object.assign({ sessionID: caller.id }, { agent: "build", resources: ["*"] }))
      }),
    )
    expect(
      Exit.isFailure(
        yield* input.targetServices.permission
          .ask({ sessionID: target.id, action: "read", resources: ["src/main.go"] })
          .pipe(Effect.exit),
      ),
    ).toBe(true)
    expect(yield* input.targetServices.permission.list()).toHaveLength(0)
  }),
)

for (const late of [false, true])
  it.live(`effect validates caller external boundary (${late ? "late authority" : "direct effect"})`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      yield* input.sessions.setPermissions({
        sessionID: caller.id,
        permissions: [
          { action: "*", resource: "*", effect: "allow" },
          { action: "external_directory", resource: "*", effect: "deny" },
        ],
      })
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id && (!late || event.phase === "effect"))
            event.requireAuthority({ sessionID: caller.id })
        }),
      )
      const episode = yield* input.targetServices.operations.begin(
        {
          kind: "file-path",
          action: "read",
          targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
        },
        { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
      )
      if (late) yield* input.targetServices.operations.authorizeBoundary(episode)
      expect(Exit.isFailure(yield* input.targetServices.operations.authorizeEffect(episode).pipe(Effect.exit))).toBe(
        true,
      )
    }),
  )

it.live("external boundary once is reused only within one episode, not the next same-call operation", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.sessions.setPermissions({
      sessionID: caller.id,
      permissions: [
        { action: "*", resource: "*", effect: "allow" },
        { action: "external_directory", resource: "*", effect: "ask" },
      ],
    })
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
      }),
    )
    const bus = yield* Bus.Service
    const asked = yield* Deferred.make<Permission.Request>()
    const repeated = yield* Deferred.make<Permission.Request>()
    let count = 0
    yield* bus.subscribe(Permission.Event.Asked).pipe(
      Stream.runForEach((event) => {
        if (event.data.sessionID !== caller.id) return Effect.void
        count++
        return count === 1
          ? Deferred.succeed(asked, event.data).pipe(Effect.asVoid)
          : Deferred.succeed(repeated, event.data).pipe(Effect.asVoid)
      }),
      Effect.forkScoped({ startImmediately: true }),
    )
    const invoke = () =>
      Effect.gen(function* () {
        const episode = yield* input.targetServices.operations.begin(
          {
            kind: "file-path",
            action: "read",
            targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
          },
          { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
        )
        yield* input.targetServices.operations.authorizeBoundary(episode)
        yield* input.targetServices.operations.authorizeEffect(episode)
      })
    const first = yield* invoke().pipe(Effect.forkScoped({ startImmediately: true }))
    const request = yield* Deferred.await(asked)
    expect(request.action).toBe("external_directory")
    yield* input.callerServices.permission.reply({ requestID: request.id, reply: "once" })
    const firstResult = yield* Effect.raceFirst(
      Fiber.await(first).pipe(Effect.map((exit) => ({ kind: "ended" as const, exit }))),
      Deferred.await(repeated).pipe(Effect.map(() => ({ kind: "asked" as const }))),
    )
    expect(firstResult.kind).toBe("ended")
    if (firstResult.kind !== "ended") return
    expect(Exit.isSuccess(firstResult.exit)).toBe(true)
    expect(count).toBe(1)
    const second = yield* invoke().pipe(Effect.forkScoped({ startImmediately: true }))
    const secondResult = yield* Effect.raceFirst(
      Fiber.await(second).pipe(Effect.as("ended" as const)),
      Deferred.await(repeated).pipe(Effect.as("asked" as const)),
    )
    expect(secondResult).toBe("asked")
    yield* input.callerServices.permission.reply({ requestID: (yield* Deferred.await(repeated)).id, reply: "once" })
    yield* Fiber.join(second)
    expect(count).toBe(2)
  }),
)

for (const ceiling of ["caller", "target"] as const)
  it.live(`native ${ceiling} narrowing hook remains final for operation`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      yield* (ceiling === "caller" ? input.callerServices.hooks : input.targetServices.hooks).register(
        "permission",
        "evaluate",
        (event) =>
          Effect.sync(() => {
            if (event.action === "read") {
              event.effect = "deny"
              event.message = `${ceiling} ceiling`
            }
          }),
      )
      const episode = yield* input.targetServices.operations.begin(
        {
          kind: "file-path",
          action: "read",
          targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
        },
        { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
      )
      const failure = yield* Effect.gen(function* () {
        yield* input.targetServices.operations.authorizeBoundary(episode)
        yield* input.targetServices.operations.authorizeEffect(episode)
      }).pipe(Effect.flip)
      expect(failure).toBeInstanceOf(Permission.BlockedError)
      expect(failure.message).toBe(`${ceiling} ceiling`)
      expect(yield* input.callerServices.permission.list()).toHaveLength(0)
      expect(yield* input.targetServices.permission.list()).toHaveLength(0)
    }),
  )

it.live("missing required native principal fails before admission or asks", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    const missing = Session.ID.create()
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === target.id) event.requireAuthority({ sessionID: missing })
      }),
    )
    const episode = yield* input.targetServices.operations.begin(
      {
        kind: "file-path",
        action: "read",
        targets: [{ absolute: AbsolutePath.make(path.join(input.directories[1], "src/main.go")), kind: "file" }],
      },
      { ...toolIdentity, sessionID: target.id, id: Tool.CallID.make("same-call") },
    )
    const failure = yield* input.targetServices.operations.authorizeBoundary(episode).pipe(Effect.flip)
    expect(failure.message).toBe("Required authority session is missing")
    expect(yield* input.targetServices.permission.list()).toHaveLength(0)
  }),
)
