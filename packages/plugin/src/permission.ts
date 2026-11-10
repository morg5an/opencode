import { Agent } from "@opencode/schema/agent"
import { Permission } from "@opencode/schema/permission"
import { Schema } from "effect"
import { Session } from "@opencode/schema/session"

export const AuthorityInput = Schema.Struct({ sessionID: Session.ID })
export interface AuthorityInput extends Schema.Schema.Type<typeof AuthorityInput> {}

export interface PermissionOperationEvaluation {
  readonly sessionID: Session.ID
  readonly source?: Permission.Source
  readonly kind: "file-path" | "file-search" | "shell" | "unsupported"
  readonly phase: "boundary" | "effect"
  /** Experimental: add a native authority requirement for this operation; cannot grant permission. */
  readonly requireAuthority: (input: AuthorityInput) => void
}

export const AssertInput = Schema.Struct({
  sessionID: Permission.Request.fields.sessionID,
  agent: Agent.ID.pipe(Schema.optional),
  action: Permission.Request.fields.action,
  resources: Permission.Request.fields.resources,
  save: Permission.Request.fields.save,
  metadata: Permission.Request.fields.metadata,
  source: Permission.Request.fields.source,
})
export interface AssertInput extends Schema.Schema.Type<typeof AssertInput> {}
