import { expect, test } from "bun:test"
import { Schema } from "effect"
import { Permission } from "../src/permission.js"

test("permission request preserves typed experimental delegation without inventing ordinary origin", () => {
  const decode = Schema.decodeUnknownSync(Permission.Request)
  const ordinary = { id: "per_ordinary", sessionID: "ses_caller", action: "read", resources: ["/target/file.ts"] }
  expect(decode(ordinary)).not.toHaveProperty("experimental")
  const delegation = {
    operationID: "operation-private",
    targetSessionID: "ses_target",
    targetDirectory: "/target",
    source: { type: "tool", messageID: "msg_target", id: "call_target" },
  }
  const outgoing = decode({ ...ordinary, experimental: { delegation } })
  expect(outgoing).toMatchObject({ sessionID: "ses_caller", experimental: { delegation } })
  expect(outgoing).not.toHaveProperty("source")
  expect(() => decode({ ...ordinary, experimental: { delegation: { ...delegation, targetDirectory: 4 } } })).toThrow()
})
