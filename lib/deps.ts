import type { FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import { attempt } from "@/utils/attempt"

type NativeTimerArg = Parameters<typeof globalThis.clearTimeout>[0]
type NativeIntervalArg = Parameters<typeof globalThis.clearInterval>[0]

const wsCandidate = attempt(() => globalThis.WebSocket)
const cachedWebSocket: (new (url: string | URL) => WebSocket) | null =
  wsCandidate instanceof Error || typeof wsCandidate !== "function" ? null : wsCandidate

/**
 * platform 既定の IO を束ねた `FlumeRuntimeDeps`。
 * `FlumeTimerHandle` は不透明型 (`unknown`) のため、setTimeout / clearTimeout の戻り値・引数を
 * platform 型と橋渡しする際に境界で `as unknown as` を使う (IO 境界の最終手段)
 */
export function createFlumeDefaultDeps(): FlumeRuntimeDeps {
  return {
    fetch: (url, init) => globalThis.fetch(url, init),
    WebSocket: cachedWebSocket,
    now: () => Date.now(),
    random: () => Math.random(),
    setTimeout: (fn, ms): FlumeTimerHandle => globalThis.setTimeout(fn, ms),
    clearTimeout: (id) => globalThis.clearTimeout(id as unknown as NativeTimerArg),
    setInterval: (fn, ms): FlumeTimerHandle => globalThis.setInterval(fn, ms),
    clearInterval: (id) => globalThis.clearInterval(id as unknown as NativeIntervalArg),
  }
}
