import { describe, expect, it } from "vitest"
import { snapshotValue } from "@/utils/snapshot-value"

describe("snapshotValue", () => {
  it("copies deep graphs without recursion and preserves cycles, holes and shared references", () => {
    const root: Record<string, unknown> = {}
    let cursor = root
    for (let index = 0; index < 20_000; index++) {
      const child = {}
      cursor.child = child
      cursor = child
    }
    cursor.root = root
    root.shared = root.child
    const sparse: unknown[] = []
    sparse.length = 3
    root.array = sparse
    Object.defineProperty(root, "__proto__", { value: { safe: true }, enumerable: true })
    const snapshot = snapshotValue(root)
    expect(snapshot === root).toBe(false)
    expect(snapshot.shared === snapshot.child).toBe(true)
    expect(Object.getPrototypeOf(snapshot)).toBe(Object.prototype)
    expect(Object.hasOwn(snapshot, "__proto__")).toBe(true)
    expect(snapshot.array).toHaveLength(3)
    expect(Object.hasOwn(snapshot.array as object, "0")).toBe(false)
    cursor = snapshot
    for (let index = 0; index < 20_000; index++) {
      expect(Object.isFrozen(cursor)).toBe(true)
      cursor = cursor.child as Record<string, unknown>
    }
    expect(cursor.root === snapshot).toBe(true)
    expect(Object.isFrozen(root)).toBe(false)
    expect(snapshotValue(snapshot) === snapshot).toBe(true)
  })

  it("preserves opaque causes and snapshots externally frozen accessors once", () => {
    const cause = new Error("original")
    let calls = 0
    const input = Object.freeze({
      cause,
      get value() {
        return ++calls
      },
    })
    const snapshot = snapshotValue(input)
    expect(snapshot.cause).toBe(cause)
    expect(Object.isFrozen(cause)).toBe(false)
    expect(snapshot.value).toBe(1)
    expect(snapshot.value).toBe(1)
    expect(calls).toBe(1)
  })
})
