import type { FlumeRuntimeDeps } from "@/types"

export function createFlumeDefaultDeps(): FlumeRuntimeDeps {
  return {
    fetch: (url, init) => globalThis.fetch(url, init),
    WebSocket: globalThis.WebSocket,
    now: () => Date.now(),
    random: () => Math.random(),
    setTimeout: (fn, ms) => globalThis.setTimeout(fn, ms),
    clearTimeout: (id) => globalThis.clearTimeout(id),
    setInterval: (fn, ms) => globalThis.setInterval(fn, ms),
    clearInterval: (id) => globalThis.clearInterval(id),
  }
}
