import { expect } from "bun:test"
import { Effect, Exit, Scope } from "effect"
import { PermissionOperationContext } from "@opencode/core/permission/operation-context"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { Tool } from "@opencode/core/tool"
import { toolIdentity } from "./lib/tool"
import { it } from "./lib/effect"

const invocation = { ...toolIdentity, sessionID: Session.ID.make("ses_episode"), id: Tool.CallID.make("same-call") }
const intent = () => ({
  kind: "file-path" as const,
  action: "read" as const,
  targets: [{ absolute: AbsolutePath.make("/target/src/main.go"), kind: "file" as const }],
})

it.effect("creates different episodes for two operations sharing source call identity", () =>
  Effect.gen(function* () {
    const first = yield* PermissionOperationContext.make(intent(), invocation)
    const second = yield* PermissionOperationContext.make(intent(), invocation)
    expect(first.id).not.toBe(second.id)
  }),
)

it.effect("captures immutable intent instead of retaining producer mutable arrays", () =>
  Effect.gen(function* () {
    const input = intent()
    const episode = yield* PermissionOperationContext.make(input, invocation)
    input.targets[0].absolute = AbsolutePath.make("/caller/src/main.go")
    input.targets.push({ absolute: AbsolutePath.make("/other.txt"), kind: "file" })
    const snapshot = yield* PermissionOperationContext.read(episode)
    if (snapshot.intent.kind !== "file-path") throw new Error("Expected file intent")
    expect(snapshot.intent.targets).toHaveLength(1)
    expect(snapshot.intent.targets[0].absolute).toBe(AbsolutePath.make("/target/src/main.go"))
    const target = snapshot.intent.targets[0]
    expect(() => Object.assign(target, { absolute: AbsolutePath.make("/forged.txt") })).toThrow(TypeError)
    expect(target.absolute).toBe(AbsolutePath.make("/target/src/main.go"))
  }),
)

it.effect("closing native invocation scope invalidates episode", () =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const episode = yield* PermissionOperationContext.make(intent(), invocation).pipe(
      Effect.provideService(Scope.Scope, scope),
    )
    expect(Exit.isSuccess(yield* PermissionOperationContext.read(episode).pipe(Effect.exit))).toBe(true)
    yield* Scope.close(scope, Exit.void)
    expect(Exit.isFailure(yield* PermissionOperationContext.read(episode).pipe(Effect.exit))).toBe(true)
  }),
)

it.effect("copied token fields cannot reconstruct native operation authority", () =>
  Effect.gen(function* () {
    const episode = yield* PermissionOperationContext.make(intent(), invocation)
    expect(Exit.isFailure(yield* PermissionOperationContext.read({ id: episode.id }).pipe(Effect.exit))).toBe(true)
    expect(Exit.isFailure(yield* PermissionOperationContext.read(undefined).pipe(Effect.exit))).toBe(true)
  }),
)
