import { describe, expect, it, vi } from "vitest"
import { FlumeLogger } from "@/logger"
import { FlumeStatusEmitter } from "@/source-helpers/flume-status-emitter"

const createLogger = () =>
  new FlumeLogger({
    source: "test",
    deps: { now: () => 0 },
    handler: () => {},
  })

describe("FlumeStatusEmitter", () => {
  it("commits status before publishing its log to observers", () => {
    const observed: string[] = []
    const emitter = new FlumeStatusEmitter({
      log: new FlumeLogger({
        source: "test",
        deps: { now: () => 0 },
        handler: () => observed.push(emitter.value),
      }),
    })
    emitter.set("connected")
    emitter.set("reconnecting", "network drop")
    expect(observed).toEqual(["connected", "reconnecting"])
  })

  it("starts at 'disconnected'", () => {
    const emitter = new FlumeStatusEmitter({ log: createLogger() })
    expect(emitter.value).toBe("disconnected")
  })

  it("transitions and notifies onStatus", () => {
    const onStatus = vi.fn()
    const emitter = new FlumeStatusEmitter({ log: createLogger(), onStatus })

    emitter.set("connecting")
    emitter.set("connected")

    expect(emitter.value).toBe("connected")
    expect(onStatus).toHaveBeenCalledTimes(2)
    expect(onStatus).toHaveBeenLastCalledWith("connected")
  })

  it("is idempotent for identical (status, detail) transitions", () => {
    const onStatus = vi.fn()
    const emitter = new FlumeStatusEmitter({ log: createLogger(), onStatus })

    emitter.set("connecting")
    emitter.set("connecting")

    expect(onStatus).toHaveBeenCalledTimes(1)
  })

  it("re-notifies when detail differs", () => {
    const onStatus = vi.fn()
    const emitter = new FlumeStatusEmitter({ log: createLogger(), onStatus })

    emitter.set("disconnected", "HTTP 500")
    emitter.set("disconnected", "HTTP 502")

    expect(onStatus).toHaveBeenCalledTimes(2)
  })

  it("does not propagate a throwing onStatus", () => {
    const emitter = new FlumeStatusEmitter({
      log: createLogger(),
      onStatus: () => {
        throw new Error("user boom")
      },
    })

    expect(() => emitter.set("connected")).not.toThrow()
  })
})

it("commits status before observing it and does not overwrite a reentrant transition", () => {
  const seen: string[] = []
  const holder: { emitter?: FlumeStatusEmitter } = {}
  const log = new FlumeLogger({
    source: "test",
    deps: { now: () => 0 },
    handler: (entry) => {
      if (entry.action === "status" && entry.detail?.to === "connected") {
        expect(holder.emitter?.value).toBe("connected")
        holder.emitter?.set("disconnected", "observer stopped the source")
      }
    },
  })
  const emitter = new FlumeStatusEmitter({ log, onStatus: (status) => seen.push(status) })
  holder.emitter = emitter
  emitter.set("connected")
  expect(emitter.value).toBe("disconnected")
  expect(seen).toEqual(["disconnected"])
})
