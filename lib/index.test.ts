import { describe, expect, it } from "vitest"
import {
  attempt,
  isRecord,
  safeErrorMessage,
  safeInvokeCallback,
  safeJsonParse,
  safeNormalizeError,
  safeNow,
  safeRandom,
  safeReadText,
  safeStringify,
} from "@/index"

describe("root utility exports", () => {
  it("exports the common safety utilities used by custom sources", () => {
    const utilities = [
      attempt,
      isRecord,
      safeErrorMessage,
      safeInvokeCallback,
      safeJsonParse,
      safeNormalizeError,
      safeNow,
      safeRandom,
      safeReadText,
      safeStringify,
    ]

    expect(utilities.every((value) => typeof value === "function")).toBe(true)
  })
})
