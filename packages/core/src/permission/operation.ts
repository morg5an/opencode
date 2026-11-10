export * as PermissionOperation from "./operation.js"

import { Context, Effect, Layer, Scope } from "effect"
import { makeLocationNode, Node } from "@opencode/util/effect/app-node"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { FileAccessResolve } from "../file-access-resolve.js"
import { Permission } from "../permission.js"
import { PermissionOperationContext } from "./operation-context.js"
import type { Environment } from "../environment/index.js"
import type { SessionErrors } from "../session/error.js"
import type { Instance } from "../instance.js"
import type { Session } from "@opencode/schema/session"
import { Location } from "../location.js"
import { LocationServiceMap } from "../location-service-map.js"
import { SessionStore } from "../session/store.js"
import { Agent } from "../agent.js"
import { PluginHooks } from "../plugin/hooks.js"

export type Intent = PermissionOperationContext.Intent
export type Episode = PermissionOperationContext.Episode
export interface MetadataProjection {
  readonly targets: readonly FileAccessResolve.Target[]
  readonly target: boolean
}
export type Error = Permission.Error | SessionErrors.NotFoundError | Environment.Failed | Instance.Error
export interface Interface {
  readonly enabled: Effect.Effect<boolean>
  readonly prepare: (
    intent: Intent,
    invocation: PermissionOperationContext.Invocation,
  ) => Effect.Effect<Episode | undefined, Error, Scope.Scope>
  readonly begin: (
    intent: Intent,
    invocation: PermissionOperationContext.Invocation,
  ) => Effect.Effect<Episode, Error, Scope.Scope>
  readonly authorizeBoundary: (
    episode: Episode,
    metadata?: Permission.AssertInput["metadata"],
  ) => Effect.Effect<void, Error>
  readonly authorizeEffect: (
    episode: Episode,
    metadata?: Permission.AssertInput["metadata"],
  ) => Effect.Effect<void, Error>
  readonly authorizeEffectProjected: (
    episode: Episode,
    metadata: (projection: MetadataProjection) => Permission.AssertInput["metadata"],
  ) => Effect.Effect<void, Error>
}
export class Service extends Context.Service<Service, Interface>()("@opencode/PermissionOperation") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const permission = yield* Permission.Service
    const resolver = yield* FileAccessResolve.Service
    const location = yield* Location.Service
    const sessions = yield* SessionStore.Service
    const locations = yield* LocationServiceMap.Service
    const agents = yield* Agent.Service
    const hooks = yield* PluginHooks.Service
    const episodes = new WeakMap<
      Episode,
      {
        session: Session.Info
        approvals: WeakMap<Permission.Interface, Set<string>>
        principals: Set<Session.ID>
        requirements: Map<Session.ID, Set<Session.ID>>
        placements: Map<Session.ID, Session.Info>
        agents: Map<Session.ID, Agent.ID>
        scopes: Map<Session.ID, Permission.Interface>
        boundaryMetadata?: Permission.AssertInput["metadata"]
      }
    >()
    const blocked = (reason: string) =>
      new Permission.BlockedError({ rules: [], permission: "operation", resources: [], reason })
    const sameLocation = (first: Location.Ref, second: Pick<Location.Ref, "directory" | "workspaceID">) =>
      first.directory === second.directory && first.workspaceID === second.workspaceID
    const create = Effect.fnUntraced(function* (intent: Intent, invocation: PermissionOperationContext.Invocation) {
      const session = yield* sessions.get(invocation.sessionID)
      if (!session || !sameLocation(session.location, location))
        return yield* blocked("Operation invocation does not belong to this location")
      const episode = yield* PermissionOperationContext.make(intent, invocation)
      episodes.set(episode, {
        session,
        approvals: new WeakMap(),
        principals: new Set([session.id]),
        requirements: new Map(),
        placements: new Map([[session.id, session]]),
        agents: new Map(),
        scopes: new Map(),
      })
      return episode
    })
    const begin = Effect.fn("PermissionOperation.begin")(
      (intent: Intent, invocation: PermissionOperationContext.Invocation) => create(intent, invocation),
    )
    const enabled = hooks.has("permission", "experimental.operation")
    const prepare = Effect.fn("PermissionOperation.prepare")(function* (
      intent: Intent,
      invocation: PermissionOperationContext.Invocation,
    ) {
      if (!(yield* enabled)) return
      const episode = yield* create(intent, invocation)
      const required = new Set<Session.ID>()
      for (const phase of ["boundary", "effect"] as const) {
        const collected = yield* permission
          .collect({
            sessionID: invocation.sessionID,
            agent: invocation.agent,
            source: { type: "tool", messageID: invocation.messageID, id: invocation.id },
            action: intent.kind === "shell" ? "shell" : intent.action,
            resources: [],
          })
          .pipe(
            Effect.provideService(
              PermissionOperationContext.Current,
              Object.freeze({ episode, sessionID: invocation.sessionID, phase }),
            ),
          )
        for (const sessionID of collected) required.add(sessionID)
      }
      if (required.size === 0) return
      const state = episodes.get(episode)
      if (!state) return yield* blocked("Operation preparation lost native ownership")
      state.requirements.set(invocation.sessionID, required)
      return episode
    })

    const authorize = Effect.fn("PermissionOperation.authorize")(function* (
      episode: Episode,
      phase: "boundary" | "effect",
      metadata?: Permission.AssertInput["metadata"],
      metadataProjection?: (projection: MetadataProjection) => Permission.AssertInput["metadata"],
    ) {
      const state = episodes.get(episode)
      if (!state) return yield* blocked("Operation episode was not issued by this scope")
      if (phase === "boundary" && metadata !== undefined) state.boundaryMetadata = structuredClone(metadata)
      const snapshot = yield* PermissionOperationContext.read(episode).pipe(
        Effect.mapError(() => blocked("Invalid operation episode")),
      )
      const root = yield* sessions.get(snapshot.invocation.sessionID)
      if (!root || !sameLocation(root.location, location) || root.agent !== state.session.agent)
        return yield* blocked("Operation authority moved or changed agent")

      interface Actor {
        readonly session: Session.Info
        readonly permission: Permission.Interface
        readonly resolver: FileAccessResolve.Interface
        readonly location: Location.Ref
        readonly root: boolean
        readonly agents: Agent.Interface
      }
      const validate = Effect.fnUntraced(function* (actor: Actor) {
        yield* PermissionOperationContext.read(episode).pipe(Effect.mapError(() => blocked("Closed operation episode")))
        const current = yield* sessions.get(actor.session.id)
        if (!current || !sameLocation(current.location, actor.location) || current.agent !== actor.session.agent)
          return yield* blocked("Authority moved, disappeared, or changed agent")
        const remembered = state.placements.get(current.id)
        if (remembered && (!sameLocation(current.location, remembered.location) || current.agent !== remembered.agent))
          return yield* blocked("Authority moved or changed agent during operation")
        state.placements.set(current.id, remembered ?? current)
        const scope = state.scopes.get(current.id)
        if (scope && scope !== actor.permission)
          return yield* blocked("Native authority context changed during operation")
        state.scopes.set(current.id, actor.permission)
        const selected = yield* actor.agents.resolve(actor.root ? snapshot.invocation.agent : current.agent)
        if (!selected || (state.agents.has(current.id) && state.agents.get(current.id) !== selected.id))
          return yield* blocked("Native authority agent identity changed")
        state.agents.set(current.id, selected.id)
      })
      let settlements = 0
      const walk = (actor: Actor, ancestry: readonly Session.ID[], stage = phase): Effect.Effect<void, Error> =>
        Effect.gen(function* () {
          if (stage === "effect") yield* walk(actor, ancestry, "boundary")
          yield* validate(actor)
          if (ancestry.includes(actor.session.id)) return yield* blocked("Authority cycle detected")
          if (ancestry.length >= 8 || (!state.principals.has(actor.session.id) && state.principals.size >= 16))
            return yield* blocked("Native authority limit exceeded")
          state.principals.add(actor.session.id)
          const frame = Object.freeze({
            episode,
            sessionID: actor.session.id,
            phase: stage,
            targetDirectory: root.location.directory,
          })
          const projected = yield* project(snapshot.intent, actor.resolver)
          const actorMetadata =
            stage === "effect" && metadataProjection
              ? metadataProjection({ targets: projected.targets, target: actor.root })
              : metadata
          const origin = actor.root
            ? {
                agent: snapshot.invocation.agent,
                source: { type: "tool" as const, messageID: snapshot.invocation.messageID, id: snapshot.invocation.id },
              }
            : {}
          const input = (requirement: Requirement): Permission.AssertInput => ({
            ...requirement,
            ...origin,
            sessionID: actor.session.id,
            ...((requirement.metadata ??
              (requirement.action === "external_directory" ? state.boundaryMetadata : actorMetadata)) === undefined
              ? {}
              : {
                  metadata:
                    requirement.metadata ??
                    (requirement.action === "external_directory" ? state.boundaryMetadata : actorMetadata),
                }),
          })
          if (
            stage === "boundary" &&
            projected.effect &&
            (yield* actor.permission.configuredDenies(input(projected.effect)))
          )
            return yield* blocked("Configured policy denies native operation")
          const requirements = stage === "boundary" ? projected.boundary : projected.effect ? [projected.effect] : []
          const base = input(
            projected.effect ?? {
              action: snapshot.intent.kind === "shell" ? "shell" : snapshot.intent.action,
              resources: [],
            },
          )
          const approved = state.approvals.get(actor.permission) ?? new Set<string>()
          state.approvals.set(actor.permission, approved)
          const nextPath = [...ancestry, actor.session.id]
          const inspect = () =>
            Effect.gen(function* () {
              const principals = state.requirements.get(actor.session.id) ?? new Set<Session.ID>()
              state.requirements.set(actor.session.id, principals)
              for (const principal of yield* actor.permission.collect(base)) principals.add(principal)
              const decisions: Permission.Decision[] = []
              for (const requirement of requirements) {
                const decision = yield* actor.permission.inspect(input(requirement))
                decisions.push(decision)
                if (decision.effect === "deny") break
              }
              for (const decision of decisions) for (const principal of decision.authorities) principals.add(principal)
              return { decisions, principals: [...principals] }
            }).pipe(Effect.provideService(PermissionOperationContext.Current, frame))
          const visit = (principals: readonly Session.ID[]) =>
            Effect.forEach(
              principals,
              (sessionID) =>
                Effect.scoped(
                  Effect.gen(function* () {
                    if (nextPath.includes(sessionID)) return yield* blocked("Authority cycle detected")
                    const caller = yield* sessions.get(sessionID)
                    if (!caller) return yield* blocked("Required authority session is missing")
                    const remembered = state.placements.get(caller.id)
                    if (
                      remembered &&
                      (!sameLocation(caller.location, remembered.location) || caller.agent !== remembered.agent)
                    )
                      return yield* blocked("Authority moved or changed agent during operation")
                    const context = yield* locations.contextEffect(caller.location)
                    return yield* Effect.gen(function* () {
                      const callerPermission = yield* Permission.Service
                      const callerResolver = yield* FileAccessResolve.Service
                      const callerAgents = yield* Agent.Service
                      return yield* walk(
                        {
                          session: caller,
                          permission: callerPermission,
                          resolver: callerResolver,
                          location: caller.location,
                          root: false,
                          agents: callerAgents,
                        },
                        nextPath,
                        stage,
                      )
                    }).pipe(Effect.provideContext(context))
                  }),
                ),
              { discard: true },
            )
          const key = (decision: Permission.Decision, requirement: Requirement) =>
            JSON.stringify({
              phase: requirement.action === "external_directory" ? "boundary" : stage,
              sessionID: actor.session.id,
              revision: decision.revision,
              effect: decision.effect,
              message: decision.message,
              requirement,
              metadata: input(requirement).metadata,
            })
          for (let attempt = 0; attempt < 16; attempt++) {
            yield* validate(actor)
            const before = yield* inspect()
            const denial = before.decisions.find((decision) => decision.effect === "deny")
            if (denial) return yield* actor.permission.settle(denial)
            yield* visit(before.principals)
            let waited = false
            for (const [index, decision] of before.decisions.entries()) {
              const signature = key(decision, requirements[index])
              if (decision.effect === "ask" && approved.has(signature)) continue
              if (decision.effect === "ask") settlements++
              yield* actor.permission.settle(decision).pipe(
                Effect.provideService(
                  PermissionOperationContext.Authorized,
                  Object.freeze({
                    episode,
                    sessionID: actor.session.id,
                    authorities: Object.freeze([...before.principals]),
                  }),
                ),
              )
              if (decision.effect === "ask") {
                approved.add(signature)
                waited = true
              }
            }
            if (waited) yield* visit(before.principals)
            yield* validate(actor)
            const after = yield* inspect()
            const blockedDecision = after.decisions.find((decision) => decision.effect === "deny")
            if (blockedDecision) return yield* actor.permission.settle(blockedDecision)
            if (
              JSON.stringify(after.principals) === JSON.stringify(before.principals) &&
              after.decisions.every(
                (decision, index) =>
                  key(decision, requirements[index]) === key(before.decisions[index], requirements[index]) ||
                  (decision.effect === "allow" && before.decisions[index].effect === "ask"),
              )
            )
              return
          }
          return yield* blocked("Native authority policy did not stabilize")
        })
      for (let attempt = 0; attempt < 16; attempt++) {
        const before = settlements
        yield* walk({ session: root, permission, resolver, location: root.location, root: true, agents }, [])
        // A later sibling's ask can stale an earlier subtree; finish only after a complete pass without new asks.
        if (settlements === before) return
      }
      return yield* blocked("Native authority graph did not stabilize after approval")
    })
    return Service.of({
      enabled,
      prepare,
      begin,
      authorizeBoundary: (episode, metadata) => authorize(episode, "boundary", metadata),
      authorizeEffect: (episode, metadata) => authorize(episode, "effect", metadata),
      authorizeEffectProjected: (episode, metadata) => authorize(episode, "effect", undefined, metadata),
    })
  }),
)
interface Requirement {
  readonly action: string
  readonly resources: readonly string[]
  readonly save?: readonly string[]
  readonly metadata?: Permission.AssertInput["metadata"]
}

const project = Effect.fn("PermissionOperation.project")(function* (
  intent: Intent,
  resolver: FileAccessResolve.Interface,
) {
  const targets =
    intent.kind === "file-path"
      ? yield* Effect.forEach(intent.targets, (target) =>
          resolver.resolve({ path: target.absolute, kind: target.kind }),
        )
      : intent.kind === "file-search"
        ? [yield* resolver.resolve({ path: intent.root, kind: intent.action === "glob" ? "directory" : undefined })]
        : yield* Effect.forEach([intent.cwd, ...intent.directories], (directory) =>
            resolver.resolve({ path: directory, kind: "directory" }),
          )
  const external = [
    ...new Map(
      targets
        .flatMap((target) => (target.externalDirectory ? [target.externalDirectory] : []))
        .map((boundary) => [boundary.resource, boundary]),
    ).values(),
  ]
  const boundary: Requirement[] =
    intent.kind === "file-path" && intent.boundaryHints === "per-file"
      ? targets.flatMap((target) =>
          target.externalDirectory
            ? [
                {
                  action: "external_directory",
                  resources: [target.externalDirectory.resource],
                  save: [target.externalDirectory.save],
                  metadata: { filepath: target.absolute, parentDir: target.externalDirectory.directory },
                },
              ]
            : [],
        )
      : external.length
        ? [
            {
              action: "external_directory",
              resources: external.map((item) => item.resource),
              save: external.map((item) => item.save),
            },
          ]
        : []
  const effect: Requirement | undefined =
    intent.kind === "file-path"
      ? { action: intent.action, resources: [...new Set(targets.map((target) => target.resource))], save: ["*"] }
      : intent.kind === "file-search"
        ? { action: intent.action, resources: [intent.pattern], save: ["*"] }
        : intent.commands.length
          ? {
              action: "shell",
              resources: intent.commands.map((command) => command.resource),
              save: intent.commands.map((command) => command.save),
            }
          : undefined
  return { boundary, effect, targets }
})

export const node: LayerNode.Provider<Service, never, typeof Node.tags.values.location> = makeLocationNode({
  service: Service,
  layer,
  deps: [
    Permission.node,
    FileAccessResolve.node,
    Location.node,
    SessionStore.node,
    LocationServiceMap.node,
    Agent.node,
    PluginHooks.node,
  ],
})
