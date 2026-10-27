import type {
  FlumeDeliveryStatus,
  FlumeDiagnosticSnapshot,
  FlumeErrorHandler,
  FlumeLog,
  FlumeRuntimeDeps,
  FlumeStreamHandler,
  FlumeStreamItem,
  FlumeStreamOptions,
} from "@/types"
import { FlumeStreamHub } from "@/flume-stream-hub"
import { FlumeSerialQueue } from "@/utils/serial-queue"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNow } from "@/utils/safe-now"

type Props = {
  onEvent?: FlumeStreamHandler
  onError?: FlumeErrorHandler
  callbackBuffer?: number
}

type Callback = "onEvent" | "onError"

const DIAGNOSTIC_CAPACITY = 100
const DIAGNOSTIC_CALLBACK_CAPACITY = 2

/** 配送・callback の例外隔離・終了を一箇所で所有する。Source の停止とは独立して drain する。 */
export class FlumeDelivery {
  private readonly queue = new FlumeSerialQueue()
  private readonly hub = new FlumeStreamHub({
    onDrop: (drop) => {
      this.droppedStreamItems += drop.dropped
      if (drop.first) this.streamOverflow()
    },
  })
  private readonly capacity: number
  private accepting = true
  private overflowed = false
  private droppedCallbacks = 0
  private droppedEventCallbacks = 0
  private droppedLogCallbacks = 0
  private callbackFailures = 0
  private droppedStreamItems = 0
  private pendingDiagnostics = 0
  private droppedDiagnosticCallbacks = 0
  private diagnosticCount = 0
  private readonly diagnosticEntries: Readonly<FlumeLog>[] = []

  constructor(
    private readonly props: Props,
    private readonly deps: Pick<FlumeRuntimeDeps, "now">,
  ) {
    const requested = props.callbackBuffer
    this.capacity =
      typeof requested === "number" && Number.isFinite(requested)
        ? Math.max(1, Math.floor(requested))
        : 1000
  }

  publish(item: FlumeStreamItem): void {
    if (!this.accepting) return
    if (item.kind === "log") this.remember(item.log)
    this.hub.publish(item)
    this.enqueue("onEvent", item)
    if (item.kind === "log" && item.log.level === "error") this.enqueue("onError", item)
  }

  stream(options?: FlumeStreamOptions): AsyncIterableIterator<FlumeStreamItem> {
    return this.hub.subscribe(options)
  }

  status(): FlumeDeliveryStatus {
    return Object.freeze({
      callbackBuffer: this.capacity,
      pendingCallbacks: this.queue.size,
      droppedCallbacks: this.droppedCallbacks,
      droppedEventCallbacks: this.droppedEventCallbacks,
      droppedLogCallbacks: this.droppedLogCallbacks,
      droppedDiagnosticCallbacks: this.droppedDiagnosticCallbacks,
      callbackFailures: this.callbackFailures,
      droppedStreamItems: this.droppedStreamItems,
    })
  }

  diagnostics(): FlumeDiagnosticSnapshot {
    return Object.freeze({
      capacity: DIAGNOSTIC_CAPACITY,
      total: this.diagnosticCount,
      evicted: this.diagnosticCount - this.diagnosticEntries.length,
      entries: Object.freeze([...this.diagnosticEntries]),
    })
  }

  private remember(log: FlumeLog): void {
    if (log.level !== "warn" && log.level !== "error") return
    this.diagnosticCount++
    if (this.diagnosticEntries.length >= DIAGNOSTIC_CAPACITY) this.diagnosticEntries.shift()
    this.diagnosticEntries.push(
      Object.freeze({
        ...log,
        detail: log.detail ? Object.freeze({ ...log.detail }) : undefined,
      }),
    )
  }

  close(): void {
    if (!this.accepting) return
    this.accepting = false
    void this.drain().then(() => this.hub.close())
  }

  drain(): Promise<void> {
    return this.queue.drain()
  }

  private enqueue(callback: Callback, item: FlumeStreamItem, notifyPeer = true): void {
    const invoke = attempt(() => {
      if (callback === "onEvent") {
        const handler = this.props.onEvent
        return handler ? () => handler(item) : undefined
      }
      const handler = this.props.onError
      return handler && item.kind === "log" ? () => handler(item.log) : undefined
    })
    if (invoke instanceof Error) {
      this.reportFailure(callback, invoke, item, notifyPeer)
      return
    }
    if (!invoke) return
    if (this.queue.size >= this.capacity && notifyPeer) {
      this.callbackOverflow(item)
      return
    }

    if (!notifyPeer) {
      if (this.pendingDiagnostics >= DIAGNOSTIC_CALLBACK_CAPACITY) {
        this.droppedCallbacks++
        this.droppedLogCallbacks++
        this.droppedDiagnosticCallbacks++
        return
      }
      this.pendingDiagnostics++
    }

    void this.queue.add(async () => {
      const result = await attempt(async () => invoke())
      if (!notifyPeer) this.pendingDiagnostics--
      if (result instanceof Error) this.reportFailure(callback, result, item, notifyPeer)
    })
  }

  /** 診断を失敗した callback 自身へ戻さない。peer の失敗は pull だけへ流して終端する。 */
  private reportFailure(
    callback: Callback,
    error: Error,
    item: FlumeStreamItem,
    notifyPeer: boolean,
  ): void {
    this.callbackFailures++
    const detail =
      item.kind === "event"
        ? { itemKind: item.kind, itemSource: item.event.source, itemType: item.event.type }
        : {
            itemKind: item.kind,
            itemSource: item.log.source,
            itemAction: item.log.action,
            itemDetail: item.log.detail,
          }
    const log: FlumeLog = {
      level: "error",
      errorCode: "FLUME_CALLBACK_FAILED",
      source: "flume",
      action: `${callback}.error`,
      message: `${callback} callback failed: ${safeErrorMessage({ error })}`,
      error,
      detail: { callback, ...detail },
      timestamp: safeNow({ deps: this.deps }),
    }
    this.remember(log)
    this.hub.publish({ kind: "log", log })
    if (notifyPeer)
      this.enqueue(callback === "onEvent" ? "onError" : "onEvent", { kind: "log", log }, false)
  }

  private callbackOverflow(item: FlumeStreamItem): void {
    this.droppedCallbacks++
    if (item.kind === "event") this.droppedEventCallbacks++
    else this.droppedLogCallbacks++
    if (this.overflowed) return
    this.overflowed = true
    const log: FlumeLog = {
      level: "error",
      errorCode: "FLUME_BUFFER_OVERFLOW",
      source: "flume",
      action: "callback.overflow",
      message: "callback buffer overflowed; dropping new items (notified once per Flume)",
      detail: { buffer: this.capacity },
      timestamp: safeNow({ deps: this.deps }),
    }
    this.remember(log)
    this.hub.publish({ kind: "log", log })
    // 各診断 sink に一枠確保する。遅い callback でも無制限に積まない。
    this.enqueue("onError", { kind: "log", log }, false)
    this.enqueue("onEvent", { kind: "log", log }, false)
  }

  private streamOverflow(): void {
    const log: FlumeLog = {
      level: "warn",
      errorCode: "FLUME_BUFFER_OVERFLOW",
      source: "flume",
      action: "stream.overflow",
      message:
        "stream buffer overflowed, dropping items (notified once per stream; see FlumeStreamOptions.buffer)",
      timestamp: safeNow({ deps: this.deps }),
    }
    this.remember(log)
    this.enqueue("onEvent", { kind: "log", log })
  }
}
