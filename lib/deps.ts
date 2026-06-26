import type { FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import { attempt } from "@/utils/attempt"

type NativeTimerArg = Parameters<typeof globalThis.clearTimeout>[0]
type NativeIntervalArg = Parameters<typeof globalThis.clearInterval>[0]

/**
 * `globalThis.WebSocket` の現在の値を返す。
 * 取得は呼び出しごとに行う — `createFlumeDefaultDeps()` がモジュール初期化時ではなく
 * 呼ばれた瞬間の `globalThis.WebSocket` を見るので、jsdom / happy-dom / vitest の
 * `beforeEach` で `globalThis.WebSocket` を差し込むテスト戦略がそのまま機能する。
 * `WebSocket` が無い環境 (Node の素の global など) では `null` を返す。
 */
function resolveCurrentWebSocket(): (new (url: string | URL) => WebSocket) | null {
  const candidate = attempt(() => globalThis.WebSocket)

  if (candidate instanceof Error || typeof candidate !== "function") return null

  return candidate
}

/**
 * platform 既定の IO を束ねた `FlumeRuntimeDeps`。
 * `FlumeTimerHandle` は不透明型 (`unknown`) のため、setTimeout / clearTimeout の戻り値・引数を
 * platform 型と橋渡しする際に境界で `as unknown as` を使う (IO 境界の最終手段)。
 *
 * `WebSocket` を含む全 IO は呼び出しごとに `globalThis` から引く lazy lookup。
 * モジュール初期化後に `globalThis.WebSocket` が差し替わる環境
 * (テストの `beforeEach` パッチ、jsdom などのブラウザ環境エミュレータ) でも
 * `createFlumeDefaultDeps()` が返した deps が常に最新の参照を見る。
 */
export function createFlumeDefaultDeps(): FlumeRuntimeDeps {
  return {
    fetch: (url, init) => globalThis.fetch(url, init),
    WebSocket: resolveCurrentWebSocket(),
    now: () => Date.now(),
    random: () => Math.random(),
    setTimeout: (fn, ms): FlumeTimerHandle => globalThis.setTimeout(fn, ms),
    clearTimeout: (id) => globalThis.clearTimeout(id as unknown as NativeTimerArg),
    setInterval: (fn, ms): FlumeTimerHandle => globalThis.setInterval(fn, ms),
    clearInterval: (id) => globalThis.clearInterval(id as unknown as NativeIntervalArg),
  }
}
