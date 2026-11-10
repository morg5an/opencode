import { expect } from "bun:test"
import { Effect, Exit, Layer } from "effect"
import { FileAccessResolve } from "@opencode/core/file-access-resolve"
import { Location } from "@opencode/core/location"
import { Permission } from "@opencode/core/permission"
import { AbsolutePath } from "@opencode/core/schema"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { location } from "./fixture/location"
import { it } from "./lib/effect"

it.effect("resolves native resources without constructing permission service", () =>
  Effect.gen(function* () {
    const result = yield* Effect.gen(function* () {
      const resolver = yield* FileAccessResolve.Service
      return yield* resolver.resolve({ path: "src/../README.md", kind: "file" })
    }).pipe(
      Effect.provide(
        LayerNode.compile(FileAccessResolve.node, {
          replacements: [
            Location.node.replace(
              Layer.succeed(
                Location.Service,
                Location.Service.of(location({ directory: AbsolutePath.make("/project") })),
              ),
            ),
            Permission.node.replace(
              Layer.effect(Permission.Service, Effect.die(new Error("Permission must not be constructed by resolver"))),
            ),
          ],
        }),
      ),
      Effect.exit,
    )
    expect(Exit.isSuccess(result)).toBe(true)
    if (Exit.isSuccess(result))
      expect(result.value).toEqual({ absolute: AbsolutePath.make("/project/README.md"), resource: "README.md" })
  }),
)

it.live("projects the same absolute file into each native authority location", () =>
  Effect.gen(function* () {
    const project = (directory: string) =>
      Effect.gen(function* () {
        const resolver = yield* FileAccessResolve.Service
        return yield* resolver.resolve({ path: "/target/src/main.go", kind: "file" })
      }).pipe(
        Effect.provide(
          LayerNode.compile(FileAccessResolve.node, {
            replacements: [
              Location.node.replace(
                Layer.succeed(
                  Location.Service,
                  Location.Service.of(location({ directory: AbsolutePath.make(directory) })),
                ),
              ),
            ],
          }),
        ),
      )
    const target = yield* project("/target")
    const caller = yield* project("/caller")
    expect(target.absolute).toBe(AbsolutePath.make("/target/src/main.go"))
    expect(target.resource).toBe("src/main.go")
    expect(target.externalDirectory).toBeUndefined()
    expect(caller.absolute).toBe(AbsolutePath.make("/target/src/main.go"))
    expect(caller.resource).toBe("/target/src/main.go")
    expect(caller.externalDirectory).toMatchObject({ resource: "/target/src/*", directory: "/target/src" })
  }),
)
