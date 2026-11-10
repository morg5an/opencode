import fs from "fs/promises"
import path from "path"
import { $ } from "bun"
import { Effect } from "effect"
import { Database } from "@opencode/core/database/database"
import { Bus } from "@opencode/core/bus"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { Location } from "@opencode/core/location"
import { SessionStore } from "@opencode/core/session/store"
import { Session } from "@opencode/core/session"
import { Permission } from "@opencode/core/permission"
import { PermissionOperation } from "@opencode/core/permission/operation"
import { PermissionSaved } from "@opencode/core/permission/saved"
import { Agent } from "@opencode/core/agent"
import { Plugin } from "@opencode/core/plugin"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { AbsolutePath } from "@opencode/core/schema"
import { Tool } from "@opencode/core/tool"
import { Global } from "@opencode/util/global"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { tempGlobalLayer } from "./global"
import { offlineModels } from "./models"
import { initGit } from "./git"
import { tmpdirScoped } from "./tmpdir"
import { testEffect } from "../lib/effect"

export const layer = (replacements: LayerNode.Replacements = []) =>
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      SessionStore.node,
      Session.node,
      PermissionSaved.node,
      LocationServiceMap.node,
    ]),
    [Global.node.replace(tempGlobalLayer), offlineModels, ...replacements],
  )

export const it = testEffect(layer())

export const fixture = Effect.gen(function* () {
  const temp = yield* tmpdirScoped()
  const directories = [path.join(temp.path, "caller"), path.join(temp.path, "target")]
  for (const [index, directory] of directories.entries()) {
    yield* Effect.promise(async () => {
      await fs.mkdir(path.join(directory, "src"), { recursive: true })
      await fs.writeFile(path.join(directory, "src/main.go"), index === 0 ? "caller" : "target")
      await initGit(directory)
      await $`git add src/main.go`.cwd(directory).quiet()
      await $`git commit -m ${index === 0 ? "caller-root" : "target-root"}`.cwd(directory).quiet()
    })
  }
  const locations = yield* LocationServiceMap.Service
  const sessions = yield* Session.Service
  const saved = yield* PermissionSaved.Service
  const callerRef = Location.Ref.make({ directory: AbsolutePath.make(directories[0]) })
  const targetRef = Location.Ref.make({ directory: AbsolutePath.make(directories[1]) })
  const callerContext = yield* locations.contextEffect(callerRef)
  const targetContext = yield* locations.contextEffect(targetRef)
  const actor = (effect: typeof Permission.Effect.Type, location: Location.Ref) =>
    Effect.gen(function* () {
      yield* Plugin.awaitActivation
      return yield* sessions.create({
        location,
        permissions: [
          { action: "*", resource: "*", effect: "allow" },
          { action: "read", resource: "*", effect },
        ],
      })
    })
  const targetServices = yield* Effect.gen(function* () {
    return {
      operations: yield* PermissionOperation.Service,
      hooks: yield* PluginHooks.Service,
      permission: yield* Permission.Service,
      tools: yield* Tool.Service,
    }
  }).pipe(Effect.provideContext(targetContext))
  const callerServices = yield* Effect.gen(function* () {
    return {
      permission: yield* Permission.Service,
      hooks: yield* PluginHooks.Service,
      saved,
      agents: yield* Agent.Service,
    }
  }).pipe(Effect.provideContext(callerContext))
  return {
    directories,
    sessions,
    callerRef,
    targetRef,
    callerContext,
    targetContext,
    actor,
    targetServices,
    callerServices,
  }
})
