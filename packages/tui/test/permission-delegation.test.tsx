/** @jsxImportSource @opentui/solid */
import type { PermissionRequest } from "@opencode/client"
import { testRender } from "@opentui/solid"
import { expect, test } from "bun:test"
import { createSignal, Show } from "solid-js"
import { ConfigProvider } from "../src/config"
import { ClientProvider } from "../src/context/client"
import { DataProvider } from "../src/context/data"
import { Keymap } from "../src/context/keymap"
import { LocationProvider } from "../src/context/location"
import { ThemeProvider } from "../src/context/theme"
import { PermissionPrompt } from "../src/routes/session/permission"
import { ToastProvider } from "../src/ui/toast"
import { emptyThemeSource, tmpdir } from "./fixture/fixture"
import { createApi, createEventStream, createFetch } from "./fixture/tui-client"
import { TestTuiContexts } from "./fixture/tui-environment"
import { createTuiResolvedConfig } from "./fixture/tui-runtime"

for (const width of [44, 100])
  for (const trusted of [true, false])
    test(`native delegated origin at width ${width}, trusted=${trusted}`, async () => {
      await using temp = await tmpdir()
      const delegation = {
        operationID: "op_test",
        targetSessionID: "ses_target",
        targetDirectory: "/target",
        source: { type: "tool" as const, messageID: "msg_target", id: "call_7" },
      }
      const [request, setRequest] = createSignal<PermissionRequest | undefined>({
        id: "per_caller",
        sessionID: "ses_caller",
        action: "read",
        resources: ["/target/file.ts"],
        ...(trusted ? { experimental: { delegation } } : { metadata: { delegation } }),
      })
      const replies: { route: string; decision: unknown }[] = []
      const transport = createFetch((url, input) => {
        if (url.pathname.endsWith("/reply"))
          return input.json().then((body) => {
            replies.push({ route: url.pathname, decision: body })
            setRequest(
              replies.length === 1 && trusted
                ? { id: "per_target", sessionID: "ses_target", action: "read", resources: ["file.ts"] }
                : undefined,
            )
            return new Response(null, { status: 204 })
          })
      }, createEventStream())
      const setup = await testRender(
        () => (
          <TestTuiContexts directory={temp.path} paths={{ home: temp.path, state: temp.path, worktree: temp.path }}>
            <ConfigProvider config={createTuiResolvedConfig({ animations: false })}>
              <Keymap.Provider>
                <ClientProvider api={createApi(transport.fetch)}>
                  <DataProvider directory={temp.path}>
                    <LocationProvider>
                      <ThemeProvider mode="dark" source={emptyThemeSource}>
                        <ToastProvider>
                          <Show when={request()} fallback={<textarea focused initialValue="idle caller" />} keyed>
                            {(current) => <PermissionPrompt request={current} />}
                          </Show>
                        </ToastProvider>
                      </ThemeProvider>
                    </LocationProvider>
                  </DataProvider>
                </ClientProvider>
              </Keymap.Provider>
            </ConfigProvider>
          </TestTuiContexts>
        ),
        { width, height: 40, kittyKeyboard: true },
      )
      try {
        setup.renderer.start()
        await setup.renderOnce()
        const frame = setup.captureCharFrame().replace(/\s+/g, "")
        expect(frame.includes("Delegatedoperation")).toBe(trusted)
        if (trusted) {
          expect(frame).toContain("ses_target")
          expect(frame).toContain("/target")
          expect(frame).toContain("call_7")
          expect(frame).toContain("ses_caller")
        }
        setup.mockInput.pressEnter()
        await setup.waitFor(() => replies.length === 1)
        expect(replies[0]).toEqual({
          route: "/api/session/ses_caller/permission/per_caller/reply",
          decision: { decision: "once" },
        })
        if (trusted) {
          await setup.renderOnce()
          expect(setup.captureCharFrame()).not.toContain("Delegated operation")
          setup.mockInput.pressEscape()
          await setup.waitFor(() => replies.length === 2)
          expect(replies[1]).toEqual({
            route: "/api/session/ses_target/permission/per_target/reply",
            decision: { decision: "reject" },
          })
        }
        await setup.renderOnce()
        expect(setup.captureCharFrame()).toContain("idle caller")
        expect(setup.renderer.currentFocusedEditor).toBeTruthy()
      } finally {
        setup.renderer.destroy()
      }
    })
