import { describe, it, expect } from "vitest"
import { createFlumeDefaultDeps } from "@/deps"

describe("createFlumeDefaultDeps", () => {
  it("returns object with all required fields", () => {
    const deps = createFlumeDefaultDeps()
    expect(typeof deps.fetch).toBe("function")
    expect(typeof deps.now).toBe("function")
    expect(typeof deps.random).toBe("function")
    expect(typeof deps.setTimeout).toBe("function")
    expect(typeof deps.clearTimeout).toBe("function")
    expect(typeof deps.setInterval).toBe("function")
    expect(typeof deps.clearInterval).toBe("function")
  })

  it("now() returns a timestamp", () => {
    const deps = createFlumeDefaultDeps()
    const now = deps.now()
    expect(typeof now).toBe("number")
    expect(now).toBeGreaterThan(0)
  })

  it("random() returns a number between 0 and 1", () => {
    const deps = createFlumeDefaultDeps()
    const val = deps.random()
    expect(val).toBeGreaterThanOrEqual(0)
    expect(val).toBeLessThan(1)
  })

  it("re-resolves globalThis.WebSocket on every call so test patches are honoured", () => {
    const original = globalThis.WebSocket
    class FakeWebSocket {}

    try {
      // 1) パッチ前: 元の (もしくは null) を返す
      const before = createFlumeDefaultDeps().WebSocket

      // 2) パッチ後: 新しい参照が返ること — モジュール初期化時の cache だと
      //    この期待は外れる (常に `before` と同じ参照になる)
      // biome-ignore lint/suspicious/noExplicitAny: test boundary
      globalThis.WebSocket = FakeWebSocket as any
      const afterPatch = createFlumeDefaultDeps().WebSocket
      expect(afterPatch).toBe(FakeWebSocket as unknown as typeof WebSocket)

      // 3) 戻したら戻ること
      // biome-ignore lint/suspicious/noExplicitAny: test boundary
      globalThis.WebSocket = original as any
      const afterRestore = createFlumeDefaultDeps().WebSocket
      expect(afterRestore).toBe(before)
    } finally {
      // biome-ignore lint/suspicious/noExplicitAny: test boundary
      globalThis.WebSocket = original as any
    }
  })
})
