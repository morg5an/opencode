import { expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Effect, Option, Schema } from "effect"
import { FileDiff } from "@opencode/schema/file-diff"
import { it, fixture, layer } from "./fixture/permission-operation"
import { executeTool, toolIdentity } from "./lib/tool"
import { testEffect } from "./lib/effect"
import { Environment } from "@opencode/core/environment/index"
import { Skill } from "@opencode/core/skill"
import { AbsolutePath } from "@opencode/core/schema"
import { transformEnvironmentFiles } from "./fixture/environment"

for (const enrolled of [false, true])
  it.live(
    `upstream virtual skill read preserves ordinary behavior and refuses unprojected authority (${enrolled})`,
    () =>
      Effect.gen(function* () {
        const input = yield* fixture
        const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
        const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
        yield* Effect.gen(function* () {
          const skills = yield* Skill.Service
          yield* skills.transform((editor) =>
            editor.add(
              Skill.Info.make({
                id: Skill.ID.make("g2-guide"),
                name: Skill.Name.make("G2 guide"),
                path: AbsolutePath.make("skill://g2-guide/SKILL.md"),
                content: "virtual-native-payload-73915",
              }),
            ),
          )
        }).pipe(Effect.provideContext(input.targetContext))
        const kinds: string[] = []
        yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
          Effect.sync(() => {
            if (event.sessionID !== target.id) return
            kinds.push(event.kind)
            if (enrolled) event.requireAuthority({ sessionID: caller.id })
          }),
        )
        const result = yield* executeTool(input.targetServices.tools, {
          sessionID: target.id,
          ...toolIdentity,
          call: { type: "tool-call", id: "virtual-read", name: "read", input: { path: "skill://g2-guide/SKILL.md" } },
        })
        expect(result.status).toBe(enrolled ? "error" : "completed")
        expect(JSON.stringify(result).includes("virtual-native-payload-73915")).toBe(!enrolled)
        expect(kinds).toContain("unsupported")
        expect(yield* input.targetServices.permission.list()).toHaveLength(0)
        expect(yield* input.callerServices.permission.list()).toHaveLength(0)
      }),
  )

for (const tool of ["write", "edit", "patch"] as const)
  for (const gate of ["caller-boundary", "target-boundary", "configured-edit"] as const) {
    const reads: string[] = []
    const preview = testEffect(
      layer([
        Environment.node.replace(
          transformEnvironmentFiles((files) => ({
            read: (file) =>
              Effect.suspend(() => {
                reads.push(file)
                return files.read(file)
              }),
          })),
        ),
      ]),
    )
    preview.live(`${tool} refuses ${gate} before reading existing contents`, () =>
      Effect.gen(function* () {
        reads.length = 0
        const input = yield* fixture
        const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
        const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
        const file =
          gate === "target-boundary"
            ? path.join(input.directories[0], "src/main.go")
            : path.join(input.directories[1], "src/main.go")
        yield* input.sessions.setPermissions({
          sessionID: gate === "target-boundary" || gate === "configured-edit" ? target.id : caller.id,
          permissions: [
            { action: "*", resource: "*", effect: "allow" },
            { action: gate === "configured-edit" ? "edit" : "external_directory", resource: "*", effect: "deny" },
          ],
        })
        yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
          Effect.sync(() => {
            if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
          }),
        )
        const args =
          tool === "write"
            ? { path: file, content: "changed" }
            : tool === "edit"
              ? { path: file, oldString: "target", newString: "changed" }
              : { patchText: `*** Begin Patch\n*** Delete File: ${file}\n*** End Patch` }
        reads.length = 0
        const result = yield* executeTool(input.targetServices.tools, {
          sessionID: target.id,
          ...toolIdentity,
          call: { type: "tool-call", id: "preview-gate", name: tool, input: args },
        })
        expect(result.status).toBe("error")
        expect(reads).not.toContain(file)
        expect(yield* input.callerServices.permission.list()).toHaveLength(0)
        expect(yield* input.targetServices.permission.list()).toHaveLength(0)
      }),
    )
  }

for (const policy of ["allow", "deny", "ask"] as const)
  it.live(`delegated edit miss hint requires independent caller read ${policy}`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor(policy, input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const body = "const privateMarker = 'hint-only-74321'\nconst next = 1\n"
      yield* Effect.promise(() => fs.writeFile(path.join(input.directories[1], "src/main.go"), body))
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
          id: "miss-hint",
          name: "edit",
          input: {
            path: "src/main.go",
            oldString: "const privateMarker = 'hint-only-74321'\nconst next = 2",
            newString: "changed",
          },
        },
      })
      expect(result.status).toBe("error")
      expect(JSON.stringify(result).includes("hint-only-74321")).toBe(policy === "allow")
      expect(yield* input.callerServices.permission.list()).toHaveLength(0)
      expect(yield* input.targetServices.permission.list()).toHaveLength(0)
      expect(yield* Effect.promise(() => fs.readFile(path.join(input.directories[1], "src/main.go"), "utf8"))).toBe(
        body,
      )
    }),
  )

for (const callerPolicy of ["allow", "deny"] as const)
  it.live(`native read leaf enforces caller ${callerPolicy} against actual target file`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const marker = "native target payload 910239"
      yield* Effect.promise(() => fs.writeFile(path.join(input.directories[1], "src/main.go"), marker))
      const caller = yield* input.actor(callerPolicy, input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      const result = yield* executeTool(input.targetServices.tools, {
        sessionID: target.id,
        ...toolIdentity,
        call: { type: "tool-call", id: "read-leaf", name: "read", input: { path: "src/main.go" } },
      })
      expect(result.status).toBe(callerPolicy === "allow" ? "completed" : "error")
      if (callerPolicy === "allow") expect(JSON.stringify(result.content)).toContain(marker)
      else expect(JSON.stringify(result)).not.toContain(marker)
    }),
  )

for (const policy of ["allow", "deny"] as const)
  it.live(`delegated missing write target honors caller ${policy}`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      yield* input.sessions.setPermissions({
        sessionID: caller.id,
        permissions: [
          { action: "*", resource: "*", effect: "allow" },
          { action: "edit", resource: "*", effect: policy },
        ],
      })
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      const file = path.join(input.directories[1], "src/new.go")
      const result = yield* executeTool(input.targetServices.tools, {
        sessionID: target.id,
        ...toolIdentity,
        call: {
          type: "tool-call",
          id: "missing-write",
          name: "write",
          input: { path: "src/new.go", content: "created" },
        },
      })
      expect(result.status).toBe(policy === "allow" ? "completed" : "error")
      expect(yield* Effect.promise(() => Bun.file(file).exists())).toBe(policy === "allow")
    }),
  )

for (const policy of ["allow", "deny"] as const)
  it.live(`delegated recovered read authorizes new concrete sibling (${policy})`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const recovered = path.join(input.directories[1], "src/Name\u202fHere.txt")
      yield* Effect.promise(() => fs.writeFile(recovered, "recovered-58937"))
      yield* input.sessions.setPermissions({
        sessionID: caller.id,
        permissions: [
          { action: "*", resource: "*", effect: "allow" },
          { action: "read", resource: recovered, effect: policy },
        ],
      })
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      const result = yield* executeTool(input.targetServices.tools, {
        sessionID: target.id,
        ...toolIdentity,
        call: { type: "tool-call", id: "recovered-read", name: "read", input: { path: "src/Name Here.txt" } },
      })
      expect(result.status).toBe(policy === "allow" ? "completed" : "error")
      expect(JSON.stringify(result).includes("recovered-58937")).toBe(policy === "allow")
    }),
  )

it.live("delegated read preserves native lexical symlink authorization", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
    const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    const link = path.join(input.directories[1], "src/link.go")
    yield* Effect.promise(() => fs.symlink(path.join(input.directories[0], "src/main.go"), link))
    const resources: string[] = []
    yield* input.callerServices.hooks.register("permission", "evaluate", (event) =>
      Effect.sync(() => {
        if (event.action === "read") resources.push(...event.resources)
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
      call: { type: "tool-call", id: "symlink-read", name: "read", input: { path: "src/link.go" } },
    })
    expect(result.status).toBe("completed")
    expect(resources).toContain(link)
    expect(JSON.stringify(result.content)).toContain("caller")
  }),
)

for (const policy of ["allow", "deny"] as const)
  it.live(`delegated patch rename and delete authorize every destination (${policy})`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const destination = path.join(input.directories[1], "src/renamed.go")
      yield* Effect.promise(() => fs.writeFile(path.join(input.directories[1], "src/delete.go"), "obsolete\n"))
      yield* input.sessions.setPermissions({
        sessionID: caller.id,
        permissions: [
          { action: "*", resource: "*", effect: "allow" },
          { action: "edit", resource: destination, effect: policy },
        ],
      })
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
          id: "patch-move",
          name: "patch",
          input: {
            patchText:
              "*** Begin Patch\n*** Update File: src/main.go\n*** Move to: src/renamed.go\n@@\n-target\n+changed\n*** Delete File: src/delete.go\n*** End Patch",
          },
        },
      })
      expect(result.status).toBe(policy === "allow" ? "completed" : "error")
      expect(yield* Effect.promise(() => Bun.file(destination).exists())).toBe(policy === "allow")
      expect(yield* Effect.promise(() => Bun.file(path.join(input.directories[1], "src/main.go")).exists())).toBe(
        policy !== "allow",
      )
      expect(yield* Effect.promise(() => Bun.file(path.join(input.directories[1], "src/delete.go")).exists())).toBe(
        policy !== "allow",
      )
    }),
  )

it.live("non-enrolled session keeps ordinary native read path in same location", () =>
  Effect.gen(function* () {
    const input = yield* fixture
    const enrolled = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    const control = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
    yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
      Effect.sync(() => {
        if (event.sessionID === enrolled.id) event.requireAuthority({ sessionID: enrolled.id })
      }),
    )
    const result = yield* executeTool(input.targetServices.tools, {
      sessionID: control.id,
      ...toolIdentity,
      call: { type: "tool-call", id: "read-control", name: "read", input: { path: "src/main.go" } },
    })
    expect(result.status).toBe("completed")
    expect(JSON.stringify(result.content)).toContain("target")
  }),
)

for (const tool of ["write", "edit", "patch"] as const)
  it.live(`native ${tool} preview identifies same concrete file in caller policy`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const expected = path.join(input.directories[1], "src/main.go")
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      let checked = false
      yield* input.callerServices.hooks.register("permission", "evaluate", (event) =>
        Effect.sync(() => {
          if (event.action !== "edit") return
          checked = true
          const files = Schema.decodeUnknownOption(Schema.Array(FileDiff.Info))(event.metadata?.files)
          if (Option.isNone(files) || !files.value.some((file) => file.file === expected)) {
            event.effect = "deny"
            event.message = "Wrong caller preview path"
          }
        }),
      )
      const args =
        tool === "write"
          ? { path: "src/main.go", content: "changed" }
          : tool === "edit"
            ? { path: "src/main.go", oldString: "target", newString: "changed" }
            : { patchText: "*** Begin Patch\n*** Update File: src/main.go\n@@\n-target\n+changed\n*** End Patch" }
      const result = yield* executeTool(input.targetServices.tools, {
        sessionID: target.id,
        ...toolIdentity,
        call: { type: "tool-call", id: `${tool}-preview`, name: tool, input: args },
      })
      expect(checked).toBe(true)
      expect(result.status).toBe("completed")
    }),
  )

for (const tool of ["glob", "grep"] as const)
  it.live(`native ${tool} metadata keeps actual search root in caller policy`, () =>
    Effect.gen(function* () {
      const input = yield* fixture
      const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
      const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
      const root = path.join(input.directories[1], "src")
      yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
        Effect.sync(() => {
          if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
        }),
      )
      let checked = false
      yield* input.callerServices.hooks.register("permission", "evaluate", (event) =>
        Effect.sync(() => {
          if (event.action === tool) {
            checked = true
            if (event.metadata?.path !== root) {
              event.effect = "deny"
              event.message = "Wrong caller search root"
            }
          }
        }),
      )
      const result = yield* executeTool(input.targetServices.tools, {
        sessionID: target.id,
        ...toolIdentity,
        call: {
          type: "tool-call",
          id: "search-root",
          name: tool,
          input: { path: "src", pattern: tool === "glob" ? "*.go" : "target" },
        },
      })
      expect(checked).toBe(true)
      expect(result.status).toBe("completed")
    }),
  )

for (const tool of ["write", "edit", "patch", "glob", "grep"] as const)
  for (const policy of ["allow", "deny"] as const)
    it.live(`native ${tool} leaf applies caller ${policy} before effect`, () =>
      Effect.gen(function* () {
        const input = yield* fixture
        const caller = yield* input.actor("allow", input.callerRef).pipe(Effect.provideContext(input.callerContext))
        const target = yield* input.actor("allow", input.targetRef).pipe(Effect.provideContext(input.targetContext))
        const action = ["write", "edit", "patch"].includes(tool) ? "edit" : tool
        yield* input.sessions.setPermissions({
          sessionID: caller.id,
          permissions: [
            { action: "*", resource: "*", effect: "allow" },
            { action, resource: "*", effect: policy },
          ],
        })
        yield* input.targetServices.hooks.register("permission", "experimental.operation", (event) =>
          Effect.sync(() => {
            if (event.sessionID === target.id) event.requireAuthority({ sessionID: caller.id })
          }),
        )
        const args =
          tool === "write"
            ? { path: "src/main.go", content: "changed" }
            : tool === "edit"
              ? { path: "src/main.go", oldString: "target", newString: "changed" }
              : tool === "patch"
                ? { patchText: "*** Begin Patch\n*** Update File: src/main.go\n@@\n-target\n+changed\n*** End Patch" }
                : tool === "glob"
                  ? { pattern: "src/*.go", path: "." }
                  : { pattern: "target", path: "src/main.go" }
        const result = yield* executeTool(input.targetServices.tools, {
          sessionID: target.id,
          ...toolIdentity,
          call: { type: "tool-call", id: `${tool}-leaf`, name: tool, input: args },
        })
        expect(result.status).toBe(policy === "allow" ? "completed" : "error")
        const contents = yield* Effect.promise(() =>
          fs.readFile(path.join(input.directories[1], "src/main.go"), "utf8"),
        )
        if (action === "edit" && policy === "allow") expect(contents).toContain("changed")
        else expect(contents).toBe("target")
      }),
    )
