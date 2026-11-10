import { expect, test } from "bun:test"
import { permissionPresentation } from "../../src/util/permission"
import { permissionInfo } from "../../src/mini/permission.shared"

test("mini permissions render only typed native origin, never metadata delegation", () => {
  const request = {
    id: "per_mini",
    sessionID: "ses_caller",
    action: "read",
    resources: ["/target/file"],
    metadata: { delegation: { targetSessionID: "forged" } },
  }
  expect(permissionInfo(request).lines).not.toContain("Delegated operation")
  const delegated = {
    ...request,
    experimental: { delegation: { operationID: "op_mini", targetSessionID: "ses_target", targetDirectory: "/target" } },
  }
  expect(permissionInfo(delegated).lines).toContain("Delegated operation")
  expect(permissionInfo(delegated).lines).toContain("Approval: ses_caller")
  expect(permissionInfo(delegated).lines).toContain("Target: ses_target")
})

test("preserves permission roots and self-contained metadata", () => {
  expect(permissionPresentation({ action: "external_directory", resources: ["/*"] }).title).toBe(
    "Access external directory /",
  )
  expect(permissionPresentation({ action: "external_directory", resources: ["C:/*"] }).title).toBe(
    "Access external directory C:/",
  )
  expect(
    permissionPresentation({ action: "webfetch", resources: [], metadata: { url: "https://example.com" } }),
  ).toMatchObject({
    title: "WebFetch https://example.com",
    lines: ["URL: https://example.com"],
  })
  expect(permissionPresentation({ action: "websearch", resources: [], metadata: { query: "releases" } })).toMatchObject(
    {
      title: 'Web Search "releases"',
      lines: ["Query: releases"],
    },
  )
})
