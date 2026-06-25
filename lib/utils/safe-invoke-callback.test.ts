import { describe, expect, it, vi } from "vitest"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"

describe("safeInvokeCallback", () => {
  it("invokes fn without calling onError on success", () => {
    const fn = vi.fn()
    const onError = vi.fn()
    safeInvokeCallback({ fn, onError })
    expect(fn).toHaveBeenCalledOnce()
    expect(onError).not.toHaveBeenCalled()
  })

  it("calls onError with normalized Error when fn throws synchronously", () => {
    const onError = vi.fn()
    safeInvokeCallback({
      fn: () => { throw new Error("boom") },
      onError,
    })
    expect(onError).toHaveBeenCalledOnce()
    const arg = onError.mock.calls[0]![0]
    expect(arg).toBeInstanceOf(Error)
    expect((arg as Error).message).toBe("boom")
  })

  it("calls onError with normalized Error when async fn rejects", async () => {
    const onError = vi.fn()
    const asyncFn = async (): Promise<void> => { throw new Error("async-boom") }
    safeInvokeCallback({ fn: asyncFn as unknown as () => void, onError })
    await new Promise((r) => globalThis.setTimeout(r, 0))
    expect(onError).toHaveBeenCalledOnce()
  })

  it("does not throw when onError itself throws", () => {
    expect(() =>
      safeInvokeCallback({
        fn: () => { throw new Error("boom") },
        onError: () => { throw new Error("onError-boom") },
      }),
    ).not.toThrow()
  })

  it("does not propagate when async onError throws after rejection", async () => {
    const asyncFn = async (): Promise<void> => { throw new Error("async-boom") }
    safeInvokeCallback({
      fn: asyncFn as unknown as () => void,
      onError: () => { throw new Error("onError-boom") },
    })
    await new Promise((r) => globalThis.setTimeout(r, 0))
  })
})
