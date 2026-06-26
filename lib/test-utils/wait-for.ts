/**
 * Test helper: polls `assertion()` until it stops throwing, or rejects when
 * the timeout elapses. Replaces `vi.waitFor` for the `bun:test` runner (which
 * does not implement it). Use only from tests — this file is not part of any
 * vite-plus pack entry so it never ships in the published bundle.
 *
 * - `interval` defaults to 10ms (tight enough that fake-time setTimeout
 *   chains drain quickly).
 * - `timeout` defaults to 1s (matches vi.waitFor's default).
 */
export const waitFor = async (
  assertion: () => void | Promise<void>,
  options: { interval?: number; timeout?: number } = {},
): Promise<void> => {
  const interval = options.interval ?? 10
  const timeout = options.timeout ?? 1_000
  const deadline = Date.now() + timeout

  // Surfaced via reject if the deadline passes without `assertion()` ever
  // returning cleanly. Keeping the latest assertion error around makes the
  // test failure message useful instead of "timed out".
  let lastError: unknown = new Error("waitFor: never attempted")

  while (Date.now() < deadline) {
    try {
      await assertion()
      return
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, interval))
  }

  throw lastError instanceof Error
    ? lastError
    : new Error(`waitFor: timed out after ${timeout}ms`)
}
