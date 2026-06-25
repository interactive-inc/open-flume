import { describe, expect, it } from "vitest"
import { safeErrorMessage } from "@/utils/safe-error-message"

describe("safeErrorMessage", () => {
  it("returns Error.message for normal Error", () => {
    expect(safeErrorMessage({ error: new Error("boom") })).toBe("boom")
  })

  it("returns String(value) for non-Error primitives", () => {
    expect(safeErrorMessage({ error: "raw" })).toBe("raw")
    expect(safeErrorMessage({ error: 42 })).toBe("42")
    expect(safeErrorMessage({ error: null })).toBe("null")
    expect(safeErrorMessage({ error: undefined })).toBe("undefined")
  })

  it("returns fallback when Error.message getter throws", () => {
    class CursedError extends Error {
      override get message(): string {
        throw new Error("getter-throws")
      }
    }
    expect(safeErrorMessage({ error: new CursedError() })).toBe("<unreadable error message>")
  })

  it("returns fallback when value is non-string-coercible", () => {
    const cursed = {
      [Symbol.toPrimitive]() {
        throw new Error("sym-throws")
      },
    }
    expect(safeErrorMessage({ error: cursed })).toBe("<unprintable error>")
  })

  it("returns fallback for Error with non-string message", () => {
    const err = new Error()
    Object.defineProperty(err, "message", { get: () => 42 as unknown as string })
    expect(safeErrorMessage({ error: err })).toBe("<non-string error message>")
  })
})
