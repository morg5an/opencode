import { expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { pathToFileURL } from "url"
import { Effect } from "effect"
import { Database } from "@opencode/core/database/database"
import { Bus } from "@opencode/core/bus"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { Location } from "@opencode/core/location"
import { Session } from "@opencode/core/session"
import { Permission } from "@opencode/core/permission"
import { Plugin } from "@opencode/core/plugin"
import { AbsolutePath } from "@opencode/core/schema"
import { Global } from "@opencode/util/global"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { tempGlobalLayer } from "../fixture/global"
import { offlineModels } from "../fixture/models"
import { tmpdirScoped } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, Session.node, LocationServiceMap.node]), [
    Global.node.replace(tempGlobalLayer),
    offlineModels,
  ]),
)

for (const variant of ["effect", "promise"] as const)
  it.live(`loads ${variant} plugin authority hook through native plugin activation`, () =>
    Effect.gen(function* () {
      const temp = yield* tmpdirScoped()
      const targetID = Session.ID.create()
      const callerID = Session.ID.create()
      const packageDirectory = path.join(temp.path, "authority-plugin")
      const effectURL = pathToFileURL(Bun.resolveSync("effect", import.meta.dir)).href
      const effectSource = `import {Effect} from ${JSON.stringify(effectURL)};
      export default {id:"native-authority-effect",effect:ctx=>ctx.permission.hook("experimental.operation",event=>Effect.sync(()=>{
        if(event.sessionID===ctx.options.target) event.requireAuthority({sessionID:ctx.options.caller});
      })).pipe(Effect.asVoid)};`
      const promiseSource = `export default {id:"native-authority-promise",async setup(ctx){
      await ctx.permission.hook("experimental.operation",event=>{
        if(event.sessionID===ctx.options.target) event.requireAuthority({sessionID:ctx.options.caller});
      });
    }};`
      yield* Effect.promise(async () => {
        await fs.mkdir(packageDirectory)
        await fs.writeFile(
          path.join(packageDirectory, "package.json"),
          JSON.stringify({ name: `native-authority-${variant}`, type: "module", exports: "./index.js" }),
        )
        await fs.writeFile(path.join(packageDirectory, "index.js"), variant === "effect" ? effectSource : promiseSource)
        await fs.writeFile(
          path.join(temp.path, "opencode.json"),
          JSON.stringify({
            permissions: [{ action: "*", resource: "*", effect: "allow" }],
            plugins: [{ package: packageDirectory, options: { target: targetID, caller: callerID } }],
          }),
        )
      })
      const locations = yield* LocationServiceMap.Service
      const sessions = yield* Session.Service
      const ref = Location.Ref.make({ directory: AbsolutePath.make(temp.path) })
      const context = yield* locations.contextEffect(ref)
      yield* Effect.gen(function* () {
        yield* Plugin.awaitActivation
        const permission = yield* Permission.Service
        const target = yield* sessions.create({ id: targetID, location: ref })
        const control = yield* sessions.create({ id: callerID, location: ref })
        expect(
          (yield* permission.ask({ sessionID: control.id, action: "read", resources: ["src/main.go"] })).effect,
        ).toBe("allow")
        expect(
          (yield* permission.ask({ sessionID: target.id, action: "read", resources: ["src/main.go"] })).effect,
        ).toBe("deny")
        expect(yield* permission.list()).toHaveLength(0)
      }).pipe(Effect.provideContext(context))
    }),
  )
