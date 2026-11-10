export * as FileAccess from "./file-access.js"

import { makeLocationNode } from "@opencode/util/effect/app-node"
import { Array, Context, Effect, Layer } from "effect"
import path from "path"
import type { Environment } from "./environment/index.js"
import { FileAccessResolve } from "./file-access-resolve.js"
import type { ResolveInput, Target } from "./file-access-resolve.js"
import { Permission } from "./permission.js"
import type { SessionErrors } from "./session/error.js"
import type { Tool } from "./tool.js"

export { Kind, ResolveInput, resolvePath } from "./file-access-resolve.js"
export type { ExternalDirectoryAuthorization, Target } from "./file-access-resolve.js"

export type Invocation = Pick<Tool.Context, "sessionID" | "agent" | "messageID" | "id">

export interface ReadOptions {
  /** A target already authorized by this invocation, used for filename recovery. */
  readonly siblingOf: Target
}

export interface Interface {
  /** Resolve a lexical path and its permission resources, without requesting approval. */
  readonly resolve: (input: ResolveInput) => Effect.Effect<Target, Environment.Failed>
  /** Approve external directories in one batch, preserving first-seen resource order. */
  readonly authorizeExternal: (
    targets: readonly Target[],
    context: Invocation,
    metadata?: Permission.AssertInput["metadata"],
  ) => Effect.Effect<void, Permission.Error | SessionErrors.NotFoundError>
  /** Resolve a read target and obtain external-directory approval before read approval. */
  readonly authorizeRead: (
    file: string,
    context: Invocation,
    options?: ReadOptions,
  ) => Effect.Effect<Target, Environment.Failed | Permission.Error | SessionErrors.NotFoundError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/FileAccess") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const resolver = yield* FileAccessResolve.Service
    const permission = yield* Permission.Service
    const resolve = resolver.resolve

    const authorizeExternal = Effect.fn("FileAccess.authorizeExternal")(function* (
      targets: readonly Target[],
      context: Invocation,
      metadata?: Permission.AssertInput["metadata"],
    ) {
      const external = Array.dedupeWith(
        targets.flatMap((target) => (target.externalDirectory ? [target.externalDirectory] : [])),
        (left, right) => left.resource === right.resource,
      )
      if (external.length === 0) return
      yield* permission.assert({
        action: "external_directory",
        resources: external.map((item) => item.resource),
        save: external.map((item) => item.save),
        ...(metadata === undefined ? {} : { metadata }),
        ...Permission.invocation(context),
      })
    })

    const authorizeRead = Effect.fn("FileAccess.authorizeRead")(function* (
      file: string,
      context: Invocation,
      options?: ReadOptions,
    ) {
      const target = yield* resolve({ path: file, kind: options ? "file" : undefined })
      const sibling = options && path.dirname(target.absolute) === path.dirname(options.siblingOf.absolute)

      // Filename recovery shares the directory approval, but checks the recovered file's own read rules.
      if (!sibling) yield* authorizeExternal([target], context)
      yield* permission.assert({
        action: "read",
        resources: [target.resource],
        save: ["*"],
        ...Permission.invocation(context),
      })
      return target
    })

    return Service.of({ resolve, authorizeExternal, authorizeRead })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [FileAccessResolve.node, Permission.node],
})
