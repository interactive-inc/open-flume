import { describe, expect, it, vi } from "vitest"
import { Flume } from "@/flume"
import { FlumeConfluence } from "@/flume-confluence"
import { FlumeSource } from "@/flume-source"
import { FlumeDiscordSource } from "@/discord/discord-source"
import { FlumeSlackSource } from "@/slack/slack-source"
import { FlumeGitHubSource } from "@/github/github-source"
import { FlumeTimeSource } from "@/time/time-source"
import { FlumeLogger } from "@/logger"
import { createFlumeDefaultDeps } from "@/deps"

class EmptySource extends FlumeSource {
  readonly name = "empty"
  protected async connect(_ctx: import("@/types").FlumeSourceStartContext): Promise<null> {
    return null
  }
  protected disconnect(): void {}
}

describe("configuration ownership", () => {
  const failure = new Error("configuration reader failed")
  const invalid = {
    get token(): never {
      throw failure
    },
  }

  it.each([
    ["discord", () => new FlumeDiscordSource(invalid)],
    [
      "slack",
      () =>
        new FlumeSlackSource({
          get appToken(): never {
            throw failure
          },
          botToken: "unused",
        }),
    ],
    ["github", () => new FlumeGitHubSource(invalid)],
    [
      "time",
      () =>
        new FlumeTimeSource({
          get cron(): never {
            throw failure
          },
        }),
    ],
  ] as const)(
    "returns a structured startup error from an unreadable %s configuration",
    async (_name, construct) => {
      const source = construct()
      const deps = createFlumeDefaultDeps()
      const result = await source.start({
        deps,
        log: new FlumeLogger({ source: "test", deps }),
        reconnect: null,
        onEvent: () => {},
      })
      expect(result).toMatchObject({ errorCode: "FLUME_RUNTIME_FAILED", cause: failure })
      expect(source.status()).toBe("disconnected")
      await source.stop()
    },
  )

  it.each(["sources", "deps", "onEvent", "signal", "reconnect"])(
    "captures a failing Flume %s getter without acquiring sources",
    async (key) => {
      const source = new EmptySource()
      const start = vi.spyOn(source, "start")
      const options = { sources: [source] }
      Object.defineProperty(options, key, {
        get: () => {
          throw failure
        },
      })
      const flume = new Flume(options)
      const result = await flume.open()
      expect(result).toMatchObject({ errorCode: "FLUME_RUNTIME_FAILED", cause: failure })
      expect(start).not.toHaveBeenCalled()
      expect(flume.diagnostics().entries).toContainEqual(
        expect.objectContaining({ error: result, errorCode: "FLUME_RUNTIME_FAILED" }),
      )
    },
  )

  it("keeps nested reconnect settings from construction when the caller edits the original object", async () => {
    let observed: unknown
    class ConfigSource extends EmptySource {
      protected override async connect(
        ctx: import("@/types").FlumeSourceStartContext,
      ): Promise<null> {
        observed = ctx.reconnect
        return null
      }
    }
    const reconnect = { maxAttempts: 3, baseDelay: 100, maxDelay: 1000 }
    const flume = new Flume({ sources: [new ConfigSource()], reconnect })
    reconnect.baseDelay = 1
    const running = await flume.open()
    if (running instanceof Error) throw running
    expect(observed).toEqual({ maxAttempts: 3, baseDelay: 100, maxDelay: 1000 })
    expect(Object.isFrozen(observed)).toBe(true)
    expect(Object.isFrozen(reconnect)).toBe(false)
    await running.close()
  })

  it("retains a failed nested reconnect configuration instead of silently disabling reconnect", async () => {
    const flume = new Flume({
      sources: [],
      reconnect: {
        get baseDelay(): never {
          throw failure
        },
      },
    })
    expect(await flume.open()).toMatchObject({ errorCode: "FLUME_RUNTIME_FAILED", cause: failure })
  })

  it("keeps the original host signal even if it is replaced before open", async () => {
    const original = new AbortController()
    const replacement = new AbortController()
    const options = { sources: [], signal: original.signal }
    const flume = new Flume(options)
    options.signal = replacement.signal
    const running = await flume.open()
    if (running instanceof Error) throw running
    expect(running.signal).toBe(original.signal)
    await running.close()
  })

  it("captures Confluence configuration errors without throwing from construction", async () => {
    const confluence = new FlumeConfluence({
      get onEvent(): never {
        throw failure
      },
    })
    expect(await confluence.add("group", [])).toMatchObject({
      errorCode: "FLUME_RUNTIME_FAILED",
      cause: failure,
    })
    expect(confluence.ids()).toEqual([])
    await confluence.closeAll()
  })

  it("keeps the current group when replacement options cannot be read", async () => {
    const confluence = new FlumeConfluence()
    expect(await confluence.add("group", [])).toBeNull()
    expect(
      await confluence.replace("group", [], {
        get replaceTimeoutMs(): never {
          throw failure
        },
      }),
    ).toMatchObject({ errorCode: "FLUME_RUNTIME_FAILED", cause: failure })
    expect(confluence.has("group")).toBe(true)
    await confluence.closeAll()
  })
})
