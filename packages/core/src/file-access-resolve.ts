export * as FileAccessResolve from "./file-access-resolve.js"

import { makeLocationNode } from "@opencode/util/effect/app-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { Context, Effect, Layer, Schema } from "effect"
import path from "path"
import { Environment } from "./environment/index.js"
import { Location } from "./location.js"
import { Project } from "./project.js"
import { AbsolutePath } from "./schema.js"

export const Kind = Schema.Literals(["file", "directory"])
export type Kind = typeof Kind.Type

export const ResolveInput = Schema.Struct({
  path: Schema.String,
  /** Selects the external approval boundary; it does not validate the target type. */
  kind: Kind.pipe(Schema.optional),
})
export type ResolveInput = typeof ResolveInput.Type

export interface ExternalDirectoryAuthorization {
  readonly action: "external_directory"
  /** Lexical directory used as the external approval boundary. */
  readonly directory: AbsolutePath
  readonly resource: string
  readonly save: string
}

export interface Target {
  readonly absolute: AbsolutePath
  /** Location-relative for internal paths, absolute for external paths. */
  readonly resource: string
  readonly externalDirectory?: ExternalDirectoryAuthorization
}

export interface Interface {
  readonly resolve: (input: ResolveInput) => Effect.Effect<Target, Environment.Failed>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/FileAccessResolve") {}

/** Expand a leading ~ and normalize Windows shell paths before lexical resolution. */
export const resolvePath = (directory: string, input: string, home = Global.Path.home) => {
  const normalized = FSUtil.windowsPath(input)
  return path.resolve(
    directory,
    normalized === "~"
      ? home
      : normalized.startsWith("~/") || (process.platform === "win32" && normalized.startsWith("~\\"))
        ? path.join(home, normalized.slice(2))
        : normalized,
  )
}

const slash = (value: string) => value.replaceAll("\\", "/")

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { files } = yield* Environment.Service
    const exists = (file: string) => Environment.exists(files, file)
    const location = yield* Location.Service
    const resolve = Effect.fn("FileAccess.resolve")(function* (input: ResolveInput) {
      const absolute = AbsolutePath.make(resolvePath(location.directory, input.path))
      const worktree = path.resolve(location.project.directory)
      const internal =
        FSUtil.contains(location.directory, absolute) ||
        (worktree !== path.parse(worktree).root && FSUtil.contains(worktree, absolute))
      if (internal) {
        return {
          absolute,
          resource: slash(path.relative(location.directory, absolute) || "."),
        } satisfies Target
      }
      const type =
        input.kind ??
        (yield* Environment.typeFollowing(files, absolute).pipe(
          Effect.catchTag("Environment.NotFound", () => Effect.undefined),
        ))
      const directory = AbsolutePath.make(type === "directory" ? absolute : path.dirname(absolute))
      return {
        absolute,
        resource: slash(absolute),
        externalDirectory: {
          action: "external_directory",
          directory,
          resource: slash(path.join(directory, "*")),
          save: slash(path.join((yield* Project.root({ exists }, directory)) ?? directory, "*")),
        },
      } satisfies Target
    })
    return Service.of({ resolve })
  }),
)

export const node = makeLocationNode({ service: Service, layer, deps: [Environment.node, Location.node] })
