import { describe, it, expect } from "vitest"
import ts from "typescript"
import { readdirSync, readFileSync } from "node:fs"
import { join } from "node:path"
import { FLUME_ERROR_CODES } from "@/errors/error-codes"
import { FLUME_ERROR_ACTIONS, resolveFlumeErrorCode } from "@/errors/resolve-error-code"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeLogger } from "@/logger"
import type { FlumeLog } from "@/types"

function sources(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name)
    return entry.isDirectory()
      ? sources(path)
      : path.endsWith(".ts") && !path.endsWith(".test.ts")
        ? [path]
        : []
  })
}

describe("public diagnostic contract", () => {
  it("registers every static library error action, so new paths cannot silently become generic errors", () => {
    const missing: string[] = []
    for (const path of sources("lib")) {
      const source = ts.createSourceFile(
        path,
        readFileSync(path, "utf8"),
        ts.ScriptTarget.Latest,
        true,
      )
      function visit(node: ts.Node): void {
        if (
          ts.isCallExpression(node) &&
          ts.isPropertyAccessExpression(node.expression) &&
          node.expression.name.text === "error" &&
          node.expression.expression.getText(source).endsWith("log")
        ) {
          const input = node.arguments[0]
          if (input && ts.isObjectLiteralExpression(input)) {
            const action = input.properties.find(
              (property) =>
                ts.isPropertyAssignment(property) && property.name.getText(source) === "action",
            )
            const hasCode = input.properties.some(
              (property) =>
                ts.isPropertyAssignment(property) && property.name.getText(source) === "errorCode",
            )
            if (
              action &&
              ts.isPropertyAssignment(action) &&
              ts.isStringLiteral(action.initializer) &&
              !hasCode &&
              !FLUME_ERROR_ACTIONS[action.initializer.text]
            )
              missing.push(`${path}: ${action.initializer.text}`)
          }
        }
        ts.forEachChild(node, visit)
      }
      visit(source)
    }
    expect(missing).toEqual([])
  })

  it("preserves service codes and the original cause independently of public errorCode", () => {
    const cause = new Error("connection reset")
    const error = new FlumeHttpError({
      status: 403,
      code: "missing_scope",
      message: "permission denied",
      cause,
    })
    expect(error.status).toBe(403)
    expect(error.code).toBe("missing_scope")
    expect(error.cause).toBe(cause)
    expect(resolveFlumeErrorCode("reconnect.terminal", error)).toBe("FLUME_AUTH_FAILED")
    expect(Object.isFrozen(FLUME_ERROR_CODES)).toBe(true)
  })

  it("carries request phase and page into logs without parsing messages", () => {
    const logs: FlumeLog[] = []
    const log = new FlumeLogger({
      source: "github",
      handler: (entry) => logs.push(entry),
      deps: { now: () => 1 },
    })
    const error = new FlumeConnectionError("deadline exceeded", {
      errorCode: "FLUME_TIMEOUT",
      context: { operation: "github.notifications", phase: "response body", page: 2 },
    })
    log.error({ action: "http.error", message: error.message, error })
    expect(logs[0]).toMatchObject({
      errorCode: "FLUME_TIMEOUT",
      error,
      detail: { operation: "github.notifications", phase: "response body", page: 2 },
    })
  })
})
