import { createFlumeDefaultDeps } from "@/deps"
import type { FlumeRuntimeDeps } from "@/types"

/** 実 timer を使いながら、ライブラリが所有する予約の残存を検査する。 */
export function trackedRuntime() {
  const timeouts = new Set<ReturnType<typeof setTimeout>>()
  const intervals = new Set<ReturnType<typeof setInterval>>()
  const deps: FlumeRuntimeDeps = {
    ...createFlumeDefaultDeps(),
    random: () => 0,
    setTimeout(fn, ms) {
      const handle = setTimeout(() => {
        timeouts.delete(handle)
        fn()
      }, ms)
      timeouts.add(handle)
      return handle
    },
    clearTimeout(handle) {
      timeouts.delete(handle as ReturnType<typeof setTimeout>)
      clearTimeout(handle as ReturnType<typeof setTimeout>)
    },
    setInterval(fn, ms) {
      const handle = setInterval(fn, ms)
      intervals.add(handle)
      return handle
    },
    clearInterval(handle) {
      intervals.delete(handle as ReturnType<typeof setInterval>)
      clearInterval(handle as ReturnType<typeof setInterval>)
    },
  }
  return {
    deps,
    pending: () => timeouts.size + intervals.size,
    dispose: () => {
      for (const handle of timeouts) clearTimeout(handle)
      for (const handle of intervals) clearInterval(handle)
      timeouts.clear()
      intervals.clear()
    },
  }
}
