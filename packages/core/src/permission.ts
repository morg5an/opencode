export * as Permission from "./permission.js"

import { makeLocationNode } from "@opencode/util/effect/app-node"
import { Context, Deferred, Effect, Layer, Schema } from "effect"
import { Permission } from "@opencode/schema/permission"
import type { Tool } from "@opencode/schema/tool"
import { Bus } from "./bus.js"
import { Location } from "./location.js"
import { Agent } from "./agent.js"
import { SessionErrors } from "./session/error.js"
import { SessionSchema } from "./session/schema.js"
import { SessionStore } from "./session/store.js"
import { Wildcard } from "./util/wildcard.js"
import { PermissionSaved } from "./permission/saved.js"
import { PluginHooks } from "./plugin/hooks.js"
import { AuthorityInput } from "@opencode/plugin/permission"
import { PermissionOperationContext } from "./permission/operation-context.js"

const PermissionEffect = Permission.Effect
export { PermissionEffect as Effect }
export { Rule, Ruleset } from "@opencode/schema/permission"
const missingAgentPermissions: Permission.Ruleset = [{ action: "*", resource: "*", effect: "deny" }]

export const ID = Permission.ID
export type ID = typeof ID.Type

export const Source = Permission.Source
export type Source = typeof Source.Type

export const Request = Permission.Request
export type Request = typeof Request.Type

export const Reply = Permission.Reply
export type Reply = typeof Reply.Type

export interface AssertInput extends Omit<Request, "id" | "message" | "experimental"> {
  readonly id?: ID
  readonly agent?: Agent.ID
}

/** The requester fields of a permission request made by a tool call. */
export const invocation = (context: Pick<Tool.Context, "sessionID" | "agent" | "messageID" | "id">) => ({
  sessionID: context.sessionID,
  agent: context.agent,
  source: { type: "tool" as const, messageID: context.messageID, id: context.id },
})

export interface ReplyInput {
  readonly requestID: ID
  readonly reply: Reply
  readonly message?: string
}

export interface AskResult {
  readonly id: ID
  readonly effect: Permission.Effect
}

export interface Decision {
  readonly effect: Permission.Effect
  readonly message?: string
  readonly authorities: ReadonlyArray<SessionSchema.ID>
  readonly revision: string
}

interface Evaluation {
  readonly effect: Permission.Effect
  readonly rules: Permission.Ruleset
  readonly agentID?: Agent.ID
  readonly message?: string
}

export { Event } from "@opencode/schema/permission"

/**
 * Rules that settle a request which would otherwise ask, for work nobody is present to answer, such as a schedule's
 * check. A request they do not allow is denied. Configured denies still win, as they are evaluated first.
 */
export const Unattended = Context.Reference<Permission.Ruleset | undefined>("@opencode/Permission/Unattended", {
  defaultValue: () => undefined,
})

export class DeclinedError extends Schema.TaggedError<DeclinedError>()("Permission.DeclinedError", {}) {}

export class CorrectedError extends Schema.TaggedError<CorrectedError>()("Permission.CorrectedError", {
  feedback: Schema.String,
}) {
  override get message() {
    return this.feedback
  }
}

export class BlockedError extends Schema.TaggedError<BlockedError>()("Permission.BlockedError", {
  rules: Permission.Ruleset,
  permission: Schema.String,
  resources: Schema.Array(Schema.String),
  reason: Schema.String.pipe(Schema.optional),
}) {
  override get message() {
    return this.reason ?? `Permission denied: ${this.permission}`
  }
}

export class NotFoundError extends Schema.TaggedError<NotFoundError>()("Permission.NotFoundError", {
  requestID: ID,
}) {}

export type Error = BlockedError | CorrectedError

export function evaluate(action: string, resource: string, ...rulesets: Permission.Ruleset[]): Permission.Rule {
  return (
    rulesets
      .flat()
      .findLast((rule) => Wildcard.match(action, rule.action) && Wildcard.match(resource, rule.resource)) ?? {
      action,
      resource: "*",
      effect: "ask",
    }
  )
}

export function merge(...rulesets: Permission.Ruleset[]): Permission.Ruleset {
  return rulesets.flat()
}

export interface Interface {
  readonly collect: (input: AssertInput) => Effect.Effect<ReadonlyArray<SessionSchema.ID>>
  readonly configuredDenies: (input: AssertInput) => Effect.Effect<boolean, SessionErrors.NotFoundError>
  readonly inspect: (input: AssertInput) => Effect.Effect<Decision, SessionErrors.NotFoundError>
  readonly settle: (decision: Decision) => Effect.Effect<void, Error | SessionErrors.NotFoundError>
  readonly close: Effect.Effect<void>
  readonly ask: (input: AssertInput) => Effect.Effect<AskResult, SessionErrors.NotFoundError>
  readonly assert: (input: AssertInput) => Effect.Effect<void, Error | SessionErrors.NotFoundError>
  /** Fails only when the rules deny the request; never asks the user. */
  readonly assertNotDenied: (input: AssertInput) => Effect.Effect<void, BlockedError | SessionErrors.NotFoundError>
  /** True only when the rules already allow the request; never asks the user. */
  readonly allows: (input: AssertInput) => Effect.Effect<boolean, SessionErrors.NotFoundError>
  readonly reply: (input: ReplyInput) => Effect.Effect<void, NotFoundError>
  readonly get: (id: ID) => Effect.Effect<Request | undefined>
  readonly forSession: (sessionID: SessionSchema.ID) => Effect.Effect<ReadonlyArray<Request>>
  readonly list: () => Effect.Effect<ReadonlyArray<Request>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Permission") {}

interface Pending {
  readonly request: Request
  readonly agent?: Agent.ID
  readonly deferred: Deferred.Deferred<void, DeclinedError | CorrectedError>
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const location = yield* Location.Service
    const agents = yield* Agent.Service
    const sessions = yield* SessionStore.Service
    const saved = yield* PermissionSaved.Service
    const hooks = yield* PluginHooks.Service
    const pending = new Map<ID, Pending>()
    let closed = false

    const close = Effect.gen(function* () {
      closed = true
      yield* Effect.forEach(Array.from(pending.values()), (item) =>
        bus
          .publish(Permission.Event.Replied, {
            sessionID: item.request.sessionID,
            requestID: item.request.id,
            reply: "reject",
          })
          .pipe(Effect.ensuring(Deferred.fail(item.deferred, new DeclinedError()))),
      )
      pending.clear()
    }).pipe(Effect.uninterruptible)
    yield* Effect.addFinalizer(() => close)

    const savedRules = Effect.fnUntraced(function* () {
      return (yield* saved.list({ projectID: location.project.id })).map(
        (item): Permission.Rule => ({
          action: item.action,
          resource: item.resource,
          effect: "allow",
        }),
      )
    })

    const configured = Effect.fnUntraced(function* (sessionID: SessionSchema.ID, agentID?: Agent.ID) {
      const session = yield* sessions.get(sessionID)
      if (!session) return yield* new SessionErrors.NotFoundError({ sessionID })
      const agent = yield* agents.resolve(agentID ?? session.agent)
      return {
        agentID: agent?.id,
        rules: merge(agent?.permissions ?? missingAgentPermissions, session.permissions ?? []),
      }
    })

    function denied(input: Pick<Request, "action" | "resources">, rules: Permission.Ruleset) {
      return input.resources.some((resource) => evaluate(input.action, resource, rules).effect === "deny")
    }

    function relevant(input: AssertInput, rules: Permission.Ruleset) {
      return rules.filter((rule) => Wildcard.match(input.action, rule.action))
    }

    const evaluateInput: (input: AssertInput) => Effect.Effect<Evaluation, SessionErrors.NotFoundError> =
      Effect.fnUntraced(function* (input: AssertInput) {
        const configuration = yield* configured(input.sessionID, input.agent)
        const rules = configuration.rules
        const agentID = configuration.agentID
        if (denied(input, rules)) return { effect: "deny" as const, rules, agentID }
        const all = [...rules, ...(yield* savedRules())]
        const effects = input.resources.map((resource) => evaluate(input.action, resource, all).effect)
        const effect: Permission.Effect = effects.includes("ask") ? "ask" : "allow"
        const event = yield* hooks.trigger("permission", "evaluate", {
          sessionID: input.sessionID,
          agent: input.agent,
          action: input.action,
          resources: input.resources,
          metadata: input.metadata,
          source: input.source,
          effect,
        })
        const unattended = yield* Unattended
        if (event.effect !== "ask" || unattended === undefined)
          return { effect: event.effect, message: event.message, rules: all, agentID }
        const settled = [...all, ...unattended]
        const allowed = input.resources.every(
          (resource) => evaluate(input.action, resource, settled).effect === "allow",
        )
        if (allowed) return { effect: "allow" as const, rules: all, agentID }
        return {
          effect: "deny" as const,
          message: `Permission ${input.action} needs approval, and nobody is present to give it`,
          rules: all,
          agentID,
        }
      })

    const decodeAuthority = Schema.decodeUnknownSync(AuthorityInput, { onExcessProperty: "error" })
    const collectAuthorities = Effect.fnUntraced(function* (
      input: AssertInput,
      frame?: PermissionOperationContext.Frame,
    ) {
      const snapshot =
        frame?.sessionID === input.sessionID
          ? yield* PermissionOperationContext.read(frame.episode).pipe(
              Effect.catchTag("Permission.InvalidEpisode", () => Effect.undefined),
            )
          : undefined
      const principals = new Set<SessionSchema.ID>()
      let open = true
      yield* hooks
        .trigger(
          "permission",
          "experimental.operation",
          Object.freeze({
            sessionID: input.sessionID,
            source: input.source ? Object.freeze({ ...input.source }) : undefined,
            kind: snapshot?.intent.kind ?? "unsupported",
            phase: frame?.phase ?? "effect",
            requireAuthority: (value: AuthorityInput) => {
              if (!open) throw new Error("Expired authority collector")
              principals.add(decodeAuthority(value).sessionID)
            },
          }),
        )
        .pipe(
          Effect.ensuring(
            Effect.sync(() => {
              open = false
            }),
          ),
        )
      return { authorities: Object.freeze([...principals]), frame: snapshot ? frame : undefined }
    })

    const evaluateRaw = Effect.fnUntraced(function* (input: AssertInput) {
      const result = yield* evaluateInput(input)
      if (result.effect === "deny") return result
      const collected = yield* collectAuthorities(input)
      if (collected.authorities.length === 0) return result
      return { ...result, effect: "deny" as const, message: "Native operation projection required" }
    })

    function request(input: AssertInput, message?: string, delegation?: Permission.Delegation): Request {
      return {
        id: input.id ?? ID.create(),
        sessionID: input.sessionID,
        action: input.action,
        resources: input.resources,
        save: input.save,
        // Tools copy optional inputs straight into metadata; undefined values are not JSON and would
        // fail the HttpApi response encoder when clients list pending requests.
        metadata:
          input.metadata &&
          Object.fromEntries(Object.entries(input.metadata).filter(([, value]) => value !== undefined)),
        source: input.source,
        message,
        ...(delegation ? { experimental: { delegation } } : {}),
      }
    }

    const create = (request: Request, agent?: Agent.ID) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const deferred = yield* Deferred.make<void, DeclinedError | CorrectedError>()
          const item = { request, agent, deferred }
          if (closed) {
            yield* Deferred.fail(deferred, new DeclinedError())
            return item
          }
          if (pending.has(request.id))
            return yield* Effect.die(new Error(`Duplicate pending permission ID: ${request.id}`))
          pending.set(request.id, item)
          yield* bus
            .publish(Permission.Event.Asked, request)
            .pipe(Effect.onError(() => Effect.sync(() => pending.delete(request.id))))
          return item
        }),
      )

    const ask = Effect.fn("Permission.ask")(function* (input: AssertInput) {
      if (closed) return { id: input.id ?? ID.create(), effect: "deny" as const }
      const result = yield* evaluateRaw(input)
      const value = request(input, result.message)
      if (result.effect === "ask") yield* create(value, input.agent)
      return { id: value.id, effect: result.effect }
    })

    const settleInput = (
      input: AssertInput,
      result: Effect.Success<ReturnType<typeof evaluateInput>>,
      delegation?: Permission.Delegation,
    ) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (result.effect === "deny") {
            return yield* new BlockedError({
              rules: relevant(input, result.rules),
              permission: input.action,
              resources: input.resources,
              reason: result.message,
            })
          }
          if (result.effect === "allow") return
          const item = yield* create(request(input, result.message, delegation), input.agent)
          return yield* restore(Deferred.await(item.deferred)).pipe(
            // Deliberate defect tunnel: leaves wrap execution in blanket `mapError`, which
            // must not convert a user's decline into model-facing tool output. The decline
            // resurfaces as a typed failure at SessionModelRequest.executeTool. A decline
            // WITH feedback (CorrectedError) intentionally stays typed so the leaf can turn
            // it into ToolFailure and the model continues.
            Effect.catchTag("Permission.DeclinedError", (error) => Effect.die(error)),
            Effect.ensuring(
              Effect.sync(() => {
                pending.delete(item.request.id)
              }),
            ),
          )
        }),
      )

    const assert = Effect.fn("Permission.assert")((input: AssertInput) =>
      Effect.gen(function* () {
        if (closed) return yield* Effect.die(new DeclinedError())
        const result = yield* evaluateRaw(input)
        return yield* settleInput(input, result)
      }),
    )

    const decisions = new WeakMap<
      Decision,
      {
        input: AssertInput
        result: Effect.Success<ReturnType<typeof evaluateInput>>
        authorities: readonly SessionSchema.ID[]
        frame?: PermissionOperationContext.Frame
      }
    >()
    const inspect = Effect.fn("Permission.inspect")(function* (input: AssertInput) {
      const owned = {
        ...input,
        resources: Object.freeze([...input.resources]),
        ...(input.save ? { save: Object.freeze([...input.save]) } : {}),
        ...(input.source ? { source: Object.freeze({ ...input.source }) } : {}),
      }
      const result = closed
        ? { effect: "deny" as const, rules: [], agentID: undefined, message: "Permission scope is closed" }
        : yield* evaluateInput(owned)
      const frame = yield* PermissionOperationContext.Current
      const collected =
        result.effect === "deny" ? { authorities: [], frame: undefined } : yield* collectAuthorities(owned, frame)
      const evaluated =
        collected.authorities.length > 0 && !collected.frame
          ? { ...result, effect: "deny" as const, message: "Native operation projection required" }
          : result
      const generation = yield* hooks.generation
      const payload = new TextEncoder().encode(
        JSON.stringify({
          rules: evaluated.rules,
          agentID: "agentID" in evaluated ? evaluated.agentID : undefined,
          generation,
        }),
      )
      const bytes = yield* Effect.promise(() => crypto.subtle.digest("SHA-256", payload))
      const revision = Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join("")
      const decision = Object.freeze({
        effect: evaluated.effect,
        message: evaluated.message,
        authorities: collected.authorities,
        revision,
      })
      decisions.set(decision, {
        input: owned,
        result: evaluated,
        authorities: collected.authorities,
        frame: collected.frame,
      })
      return decision
    })
    const settle = Effect.fn("Permission.settle")(function* (decision: Decision) {
      const owned = decisions.get(decision)
      if (!owned || closed)
        return yield* new BlockedError({
          rules: [],
          permission: owned?.input.action ?? "unknown",
          resources: owned?.input.resources ?? [],
          reason: "Permission decision is unowned, consumed, or closed",
        })
      if (owned.frame) {
        const active = yield* PermissionOperationContext.read(owned.frame.episode).pipe(
          Effect.catchTag("Permission.InvalidEpisode", () => Effect.undefined),
        )
        const authorization = yield* PermissionOperationContext.Authorized
        if (
          !active ||
          (owned.result.effect !== "deny" &&
            owned.authorities.length > 0 &&
            (!authorization ||
              authorization.episode !== owned.frame.episode ||
              authorization.sessionID !== owned.input.sessionID ||
              !owned.authorities.every((principal) => authorization.authorities.includes(principal))))
        )
          return yield* new BlockedError({
            rules: [],
            permission: owned.input.action,
            resources: owned.input.resources,
            reason: "Additional native authorities are not satisfied",
          })
      }
      decisions.delete(decision)
      const snapshot = owned.frame
        ? yield* PermissionOperationContext.read(owned.frame.episode).pipe(
            Effect.catchTag("Permission.InvalidEpisode", () => Effect.undefined),
          )
        : undefined
      const delegation: Permission.Delegation | undefined =
        snapshot && owned.frame?.targetDirectory && snapshot.invocation.sessionID !== owned.input.sessionID
          ? {
              operationID: snapshot.id,
              targetSessionID: snapshot.invocation.sessionID,
              targetDirectory: owned.frame.targetDirectory,
              source: { type: "tool", messageID: snapshot.invocation.messageID, id: snapshot.invocation.id },
            }
          : undefined
      return yield* settleInput(owned.input, owned.result, delegation)
    })

    const assertNotDenied = Effect.fn("Permission.assertNotDenied")(function* (input: AssertInput) {
      const result = yield* evaluateRaw(input)
      if (result.effect !== "deny") return
      return yield* new BlockedError({
        rules: relevant(input, result.rules),
        permission: input.action,
        resources: input.resources,
        reason: result.message,
      })
    })

    const allows = Effect.fn("Permission.allows")(function* (input: AssertInput) {
      if (closed) return false
      return (yield* evaluateRaw(input)).effect === "allow"
    })

    const reply = Effect.fn("Permission.reply")((input: ReplyInput) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const existing = pending.get(input.requestID)
          if (!existing) return yield* new NotFoundError({ requestID: input.requestID })
          yield* bus.publish(Permission.Event.Replied, {
            sessionID: existing.request.sessionID,
            requestID: existing.request.id,
            reply: input.reply,
          })

          if (input.reply === "reject") {
            yield* Deferred.fail(
              existing.deferred,
              input.message ? new CorrectedError({ feedback: input.message }) : new DeclinedError(),
            )
            pending.delete(input.requestID)
            for (const [id, item] of pending) {
              if (item.request.sessionID !== existing.request.sessionID) continue
              yield* bus.publish(Permission.Event.Replied, {
                sessionID: item.request.sessionID,
                requestID: item.request.id,
                reply: "reject",
              })
              // Feedback applies to the whole batch, so parallel asks don't end the step.
              yield* Deferred.fail(
                item.deferred,
                input.message ? new CorrectedError({ feedback: input.message }) : new DeclinedError(),
              )
              pending.delete(id)
            }
            return
          }

          if (input.reply === "always" && existing.request.save?.length) {
            yield* saved.add({
              projectID: location.project.id,
              action: existing.request.action,
              resources: existing.request.save,
            })
          }
          yield* Deferred.succeed(existing.deferred, undefined)
          pending.delete(input.requestID)
          if (input.reply !== "always" || !existing.request.save?.length) return

          for (const [id, item] of pending) {
            const result = yield* evaluateInput({ ...item.request, agent: item.agent }).pipe(
              Effect.catchTag("Session.NotFoundError", () => Effect.undefined),
            )
            if (result?.effect !== "allow") continue
            yield* bus.publish(Permission.Event.Replied, {
              sessionID: item.request.sessionID,
              requestID: item.request.id,
              reply: "always",
            })
            yield* Deferred.succeed(item.deferred, undefined)
            pending.delete(id)
          }
        }),
      ),
    )

    const list = Effect.fn("Permission.list")(function* () {
      return Array.from(pending.values(), (item) => item.request)
    })

    const get = Effect.fn("Permission.get")(function* (id: ID) {
      return pending.get(id)?.request
    })

    const forSession = Effect.fn("Permission.forSession")(function* (sessionID: SessionSchema.ID) {
      return Array.from(pending.values(), (item) => item.request).filter((request) => request.sessionID === sessionID)
    })

    const collect = Effect.fn("Permission.collect")(function* (input: AssertInput) {
      const frame = yield* PermissionOperationContext.Current
      return (yield* collectAuthorities(input, frame)).authorities
    })
    const configuredDenies = Effect.fn("Permission.configuredDenies")(function* (input: AssertInput) {
      return denied(input, (yield* configured(input.sessionID, input.agent)).rules)
    })
    return Service.of({
      ask,
      assert,
      assertNotDenied,
      allows,
      reply,
      get,
      forSession,
      list,
      close,
      inspect,
      settle,
      collect,
      configuredDenies,
    })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Bus.node, Location.node, Agent.node, SessionStore.node, PermissionSaved.node, PluginHooks.node],
})
