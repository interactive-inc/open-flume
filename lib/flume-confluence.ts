import type {
  FlumeConfluenceItem,
  FlumeConfluenceItemHandler,
  FlumeErrorHandler,
  FlumeObservation,
  FlumeReconnectOptions,
  FlumeRuntimeDeps,
  FlumeStreamHandler,
  FlumeStreamItem,
} from "@/types"
import type { FlumeSource } from "@/flume-source"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeStartError } from "@/errors/start-error"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"
import { attempt } from "@/utils/attempt"

const DEFAULT_REPLACE_TIMEOUT_MS = 10_000

type Props = {
  /**
   * 配下の全 Flume の firehose をここへ合流させる単一 sink。
   * 各 item には発信元グループの id が `groupId` としてスタンプされる
   */
  onEvent?: FlumeConfluenceItemHandler
  /** error レベル log だけ (全 Flume 共通) */
  onError?: FlumeErrorHandler
  callbackBuffer?: number
  deps?: FlumeRuntimeDeps
  reconnect?: boolean | FlumeReconnectOptions
}

type Opening = {
  controller: AbortController
  done: Promise<void>
  finish: () => void
}

type Group = {
  running: FlumeRunning | null
  opening: Opening | null
  closing: Promise<void> | null
}

/**
 * グループごとに起動・稼働・停止を所有する。replace は新しい接続が成功した時だけ交換する。
 * remove は起動中の接続も中断して停止を待つ。closeAll は終端操作。
 */
export class FlumeConfluence {
  private readonly groups = new Map<string, Group>()
  private closePromise: Promise<void> | null = null
  private readonly deps: FlumeRuntimeDeps

  constructor(private readonly props: Props = {}) {
    this.deps = props.deps ?? createFlumeDefaultDeps()
  }

  get isClosed(): boolean {
    return this.closePromise !== null
  }

  async add(id: string, sources: ReadonlyArray<FlumeSource>): Promise<Error | null> {
    if (this.isClosed) return new FlumeStartError(`FlumeConfluence: already closed: ${id}`)
    if (this.groups.has(id)) return new FlumeStartError(`FlumeConfluence: id already added: ${id}`)

    const group: Group = { running: null, opening: null, closing: null }
    this.groups.set(id, group)
    const result = await this.startGroup(id, group, sources)
    if (result instanceof Error && !group.closing) this.groups.delete(id)
    return result
  }

  /** 起動失敗時は旧接続を維持する。同じグループの並行 replace は拒否する。 */
  async replace(
    id: string,
    sources: ReadonlyArray<FlumeSource>,
    options?: { readonly replaceTimeoutMs?: number },
  ): Promise<Error | null> {
    if (this.isClosed) return new FlumeStartError(`FlumeConfluence: already closed: ${id}`)
    const group = this.groups.get(id)
    if (!group?.running || group.closing)
      return new FlumeStartError(`FlumeConfluence: id not running: ${id}`)
    if (group.opening)
      return new FlumeStartError(`FlumeConfluence: replacement already pending: ${id}`)

    const requested = options?.replaceTimeoutMs
    const timeoutMs =
      typeof requested === "number" && Number.isFinite(requested) && requested > 0
        ? Math.min(requested, 2_147_483_647)
        : DEFAULT_REPLACE_TIMEOUT_MS
    return this.startGroup(id, group, sources, timeoutMs)
  }

  remove(id: string): Promise<void> {
    const group = this.groups.get(id)
    if (!group) return Promise.resolve()
    if (group.closing) return group.closing

    // user callback が remove を再入しても同じ完了を待てるよう、副作用より先に登録する。
    group.closing = Promise.resolve().then(async () => {
      const opening = group.opening
      if (opening) attempt(() => opening.controller.abort())
      await Promise.all([opening?.done, group.running?.close()])
      this.groups.delete(id)
    })
    return group.closing
  }

  closeAll(): Promise<void> {
    if (!this.closePromise) {
      this.closePromise = Promise.resolve().then(async () => {
        await Promise.all([...this.groups.keys()].map((id) => this.remove(id)))
      })
    }
    return this.closePromise
  }

  has(id: string): boolean {
    const group = this.groups.get(id)
    return group?.running !== null && group !== undefined && group.closing === null
  }

  ids(): ReadonlyArray<string> {
    return [...this.groups.keys()].filter((id) => this.has(id))
  }

  /** 現在の接続世代を観測する。取得済みハンドルは replace / remove 後も利用できる。 */
  observe(id: string): FlumeObservation | undefined {
    const running = this.groups.get(id)?.running
    if (!running) return undefined
    return Object.freeze({
      deliveryStatus: () => running.deliveryStatus(),
      diagnostics: () => running.diagnostics(),
    })
  }

  private async startGroup(
    id: string,
    group: Group,
    sources: ReadonlyArray<FlumeSource>,
    timeoutMs?: number,
  ): Promise<Error | null> {
    const completion = Promise.withResolvers<void>()
    const opening: Opening = {
      controller: new AbortController(),
      done: completion.promise,
      finish: completion.resolve,
    }
    group.opening = opening
    const result = await attempt(async () => {
      const next = await new Flume({
        sources,
        onEvent: this.wrapOnEvent(id),
        onError: this.wrapOnError(id),
        callbackBuffer: this.props.callbackBuffer,
        deps: this.deps,
        reconnect: this.props.reconnect,
        signal: opening.controller.signal,
        openTimeoutMs: timeoutMs,
      }).open()
      if (next instanceof Error) return next
      if (this.isClosed || group.closing) {
        await next.close()
        return new FlumeStartError(`FlumeConfluence: removed during open: ${id}`)
      }
      const previous = group.running
      group.running = next
      await previous?.close()
      return null
    })
    group.opening = null
    opening.finish()
    return result
  }

  private wrapOnEvent(id: string): FlumeStreamHandler | undefined {
    const onEvent = this.props.onEvent
    if (!onEvent) return undefined

    return (item: FlumeStreamItem) => {
      const stamped: FlumeConfluenceItem = { ...item, groupId: id }
      return onEvent(stamped)
    }
  }

  private wrapOnError(id: string): FlumeErrorHandler | undefined {
    const onError = this.props.onError
    if (!onError) return undefined

    return (log) => onError({ ...log, detail: { ...log.detail, groupId: id } })
  }
}
