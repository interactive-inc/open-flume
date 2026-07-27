import { describe, it, expect, vi } from "vitest"
import { FlumeParseError } from "@/errors/parse-error"
import { parseCron } from "@/time/parse-cron"
import { FlumeTimeScheduler } from "@/time/time-scheduler"
import type { FlumeLog, FlumeRuntimeDeps } from "@/types"

const timerHandle = globalThis.setTimeout(() => {}, 0)
globalThis.clearTimeout(timerHandle)

const HOUR_MS = 60 * 60_000

function cron(expression: string) {
  const result = parseCron(expression)
  if (result instanceof FlumeParseError) throw result
  return result
}

function createMockDeps(startMs: number) {
  let nowMs = startMs
  let lastCallback: (() => void) | null = null
  const delays: number[] = []

  const deps: FlumeRuntimeDeps = {
    fetch: vi.fn(),
    now: () => nowMs,
    setTimeout: vi.fn((fn: () => void, ms: number) => {
      lastCallback = fn
      delays.push(ms)
      return timerHandle
    }),
    clearTimeout: vi.fn(),
    setInterval: vi.fn(() => timerHandle),
    clearInterval: vi.fn(),
    random: () => 0.5,
    WebSocket: globalThis.WebSocket,
  }

  return {
    deps,
    delays,
    setNow: (ms: number) => {
      nowMs = ms
    },
    fire: () => lastCallback?.(),
  }
}

describe("FlumeTimeScheduler", () => {
  it("fires a late wake once and fast-forwards past now (no replay burst)", () => {
    const test = createMockDeps(0)
    const ticks: number[] = []
    const logs: FlumeLog[] = []

    const scheduler = new FlumeTimeScheduler({
      cron: cron("* * * * *"),
      onTick: (firedAt) => ticks.push(firedAt),
      onLog: (log) => logs.push(log),
      deps: test.deps,
    })

    expect(scheduler.start(0)).toBeNull()

    // 8 時間スリープした後に起きた: 取り逃した ~480 分は burst させない
    test.setNow(8 * HOUR_MS)
    test.fire()

    expect(ticks).toEqual([60_000])

    // 次ターゲットは now 直後の分境界 (早送り済み)
    expect(test.delays[test.delays.length - 1]).toBe(60_000)

    const skipped = logs.find((log) => log.action === "scheduler.skipped")
    expect(skipped).toBeDefined()
    expect(skipped!.detail?.latenessMs).toBe(8 * HOUR_MS - 60_000)

    // now を進めずに再度起こしても burst しない
    test.fire()
    expect(ticks).toEqual([60_000])
  })

  it("does not re-arm when stop() is called from inside onTick", () => {
    const test = createMockDeps(0)
    const holder: { scheduler: FlumeTimeScheduler | null } = { scheduler: null }

    holder.scheduler = new FlumeTimeScheduler({
      cron: cron("* * * * *"),
      onTick: () => holder.scheduler?.stop(),
      deps: test.deps,
    })

    expect(holder.scheduler.start(0)).toBeNull()
    expect(test.deps.setTimeout).toHaveBeenCalledTimes(1)

    test.setNow(60_000)
    test.fire()

    // tick 内 stop() 後に orphan timer を残さない
    expect(test.deps.setTimeout).toHaveBeenCalledTimes(1)
    expect(holder.scheduler.isStopped).toBe(true)
  })

  it("invokes onHalt and stops when cron-next fails mid-run", () => {
    const test = createMockDeps(0)
    const ticks: number[] = []
    let haltCount = 0

    const scheduler = new FlumeTimeScheduler({
      cron: cron("* * * * *"),
      onTick: (firedAt) => ticks.push(firedAt),
      onHalt: () => {
        haltCount += 1
      },
      deps: test.deps,
    })

    expect(scheduler.start(0)).toBeNull()

    // Date の表現可能上限 (8.64e15) を超えた now では cron-next が収束せずエラーになる
    test.setNow(8_700_000_000_000_000)
    test.fire()

    expect(ticks).toEqual([60_000])
    expect(haltCount).toBe(1)
    expect(scheduler.isStopped).toBe(true)
    expect(test.deps.setTimeout).toHaveBeenCalledTimes(1)
  })

  it("skips the DST fall-back duplicate occurrence (fires once per wall-clock day)", () => {
    const originalTz = process.env.TZ
    process.env.TZ = "America/New_York"

    try {
      // 2021-11-07 の fall-back: 01:30 EDT (05:30Z) と 01:30 EST (06:30Z) が両方存在する
      const firstOneThirty = Date.UTC(2021, 10, 7, 5, 30)
      const startAt = firstOneThirty - 30 * 60_000
      const test = createMockDeps(startAt)
      const ticks: number[] = []

      const scheduler = new FlumeTimeScheduler({
        cron: cron("30 1 * * *"),
        onTick: (firedAt) => ticks.push(firedAt),
        deps: test.deps,
      })

      expect(scheduler.start(startAt)).toBeNull()
      expect(test.delays[0]).toBe(30 * 60_000)

      test.setNow(firstOneThirty)
      test.fire()

      expect(ticks).toEqual([firstOneThirty])

      // 次ターゲットは 1 時間後の 01:30 EST (同一壁時計分) を飛ばして翌日 01:30
      const nextDayOneThirty = Date.UTC(2021, 10, 8, 6, 30)
      expect(test.delays[test.delays.length - 1]).toBe(nextDayOneThirty - firstOneThirty)

      // 2 つ目の 01:30 の時刻に起こされても発火しない
      test.setNow(Date.UTC(2021, 10, 7, 6, 30))
      test.fire()
      expect(ticks).toEqual([firstOneThirty])
    } finally {
      if (originalTz === undefined) {
        delete process.env.TZ
      } else {
        process.env.TZ = originalTz
      }
    }
  })

  it("skips all repeated minutes for a multi-minute DST fall-back schedule", () => {
    const originalTz = process.env.TZ
    process.env.TZ = "America/New_York"

    try {
      const firstOne = Date.UTC(2021, 10, 7, 5, 0)
      const test = createMockDeps(firstOne - 60_000)
      const ticks: number[] = []
      const scheduler = new FlumeTimeScheduler({
        cron: cron("*/15 1 * * *"),
        onTick: (firedAt) => ticks.push(firedAt),
        deps: test.deps,
      })
      expect(scheduler.start(firstOne - 60_000)).toBeNull()

      for (const minute of [0, 15, 30, 45]) {
        test.setNow(Date.UTC(2021, 10, 7, 5, minute))
        test.fire()
      }

      expect(ticks).toEqual([
        Date.UTC(2021, 10, 7, 5, 0),
        Date.UTC(2021, 10, 7, 5, 15),
        Date.UTC(2021, 10, 7, 5, 30),
        Date.UTC(2021, 10, 7, 5, 45),
      ])
      expect(test.delays[test.delays.length - 1]).toBe(24 * 60 * 60_000 + 15 * 60_000)
    } finally {
      if (originalTz === undefined) delete process.env.TZ
      else process.env.TZ = originalTz
    }
  })

  it("returns an error when the initial timer cannot be armed", () => {
    const test = createMockDeps(0)
    test.deps.setTimeout = () => {
      throw new Error("timer denied")
    }
    const scheduler = new FlumeTimeScheduler({
      cron: cron("* * * * *"),
      onTick: vi.fn(),
      deps: test.deps,
    })

    expect(scheduler.start(0)).toBeInstanceOf(Error)
    expect(scheduler.isStopped).toBe(true)
  })
})
