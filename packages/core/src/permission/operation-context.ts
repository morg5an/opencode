export * as PermissionOperationContext from "./operation-context.js"

import { Context, Effect, Schema, Scope } from "effect"
import type { AbsolutePath } from "@opencode/schema/schema"
import type { Tool } from "@opencode/schema/tool"
import type { Session } from "@opencode/schema/session"

export type Invocation = Pick<Tool.Context, "sessionID" | "agent" | "messageID" | "id">
export type Intent =
  | {
      readonly kind: "file-path"
      readonly action: "read" | "edit"
      readonly boundaryHints?: "per-file"
      readonly targets: readonly { readonly absolute: AbsolutePath; readonly kind?: "file" | "directory" }[]
    }
  | {
      readonly kind: "file-search"
      readonly action: "glob" | "grep"
      readonly root: AbsolutePath
      readonly pattern: string
    }
  | {
      readonly kind: "shell"
      readonly command: string
      readonly cwd: AbsolutePath
      readonly shell: string
      readonly env: Readonly<Record<string, string | undefined>>
      readonly directories: readonly AbsolutePath[]
      readonly commands: readonly { readonly resource: string; readonly save: string }[]
    }

export interface Episode {
  readonly id: string
}
export interface Frame {
  readonly episode: Episode
  readonly sessionID: Session.ID
  readonly phase: "boundary" | "effect"
  readonly targetDirectory?: AbsolutePath
}
export const Current = Context.Reference<Frame | undefined>("@opencode/PermissionOperation/Current", {
  defaultValue: () => undefined,
})
export interface Authorization {
  readonly episode: Episode
  readonly sessionID: Session.ID
  readonly authorities: readonly Session.ID[]
}
export const Authorized = Context.Reference<Authorization | undefined>("@opencode/PermissionOperation/Authorized", {
  defaultValue: () => undefined,
})
export interface Snapshot {
  readonly id: string
  readonly intent: Intent
  readonly invocation: Invocation
}
export class InvalidEpisode extends Schema.TaggedError<InvalidEpisode>()("Permission.InvalidEpisode", {}) {}

const snapshots = new WeakMap<object, Snapshot>()
export const make = (intent: Intent, invocation: Invocation): Effect.Effect<Episode, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const episode = Object.freeze({ id: crypto.randomUUID() })
      snapshots.set(
        episode,
        Object.freeze({ id: episode.id, intent: capture(intent), invocation: Object.freeze({ ...invocation }) }),
      )
      return episode
    }),
    (episode) =>
      Effect.sync(() => {
        snapshots.delete(episode)
      }),
  )
export const read = (episode: unknown): Effect.Effect<Snapshot, InvalidEpisode> =>
  Effect.suspend(() => {
    const snapshot = typeof episode === "object" && episode !== null ? snapshots.get(episode) : undefined
    return snapshot ? Effect.succeed(snapshot) : Effect.fail(new InvalidEpisode())
  })

function capture(intent: Intent): Intent {
  if (intent.kind === "file-path")
    return Object.freeze({
      ...intent,
      targets: Object.freeze(intent.targets.map((target) => Object.freeze({ ...target }))),
    })
  if (intent.kind === "file-search") return Object.freeze({ ...intent })
  return Object.freeze({
    ...intent,
    env: Object.freeze({ ...intent.env }),
    directories: Object.freeze([...intent.directories]),
    commands: Object.freeze(intent.commands.map((command) => Object.freeze({ ...command }))),
  })
}
