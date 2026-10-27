import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { captureConfiguration } from "@/utils/capture-configuration"
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
import { FlumeClosed } from "@/flume-closed"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"
import { snapshotValue } from "@/utils/snapshot-value"
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

export type FlumeConfluenceReplacement = Readonly<{ previous: FlumeClosed }>

export type FlumeConfluenceRemoval = Readonly<{
  groupId: string
  closed: FlumeClosed | null
  /** remove が中断した起動の結果。通常の取消しも FLUME_ABORTED として返す。 */
  openingError: Error | null
}>

type Opening = {
  controller: AbortController
  done: Promise<Error | null>
  finish: (result: Error | null) => void
}

type Group = {
  running: FlumeRunning | null
  opening: Opening | null
  closing: Promise<FlumeConfluenceRemoval> | null
}

/**
 * グループごとに起動・稼働・停止を所有する。replace は新しい接続が成功した時だけ交換する。
 * remove は起動中の接続も中断して停止を待つ。closeAll は終端操作。
 */
export class FlumeConfluence {
  private readonly groups = new Map<string, Group>()
  private closePromise: Promise<ReadonlyArray<FlumeConfluenceRemoval>> | null = null
  private readonly deps: FlumeRuntimeDeps

  private readonly props: Readonly<Props> | FlumeStartError

  constructor(props: Props = {}) {
    this.props = captureConfiguration("FlumeConfluence", () => {
      return {
        onEvent: props.onEvent,
        onError: props.onError,
        callbackBuffer: props.callbackBuffer,
        deps: props.deps,
        reconnect: resolveFlumeReconnectConfig(props.reconnect) ?? false,
      }
    })
    this.deps =
      (this.props instanceof Error ? undefined : this.props.deps) ?? createFlumeDefaultDeps()
  }

  get isClosed(): boolean {
    return this.closePromise !== null
  }

  async add(id: string, sources: ReadonlyArray<FlumeSource>): Promise<Error | null> {
    if (this.props instanceof Error) return this.props
    if (this.isClosed) return new FlumeStartError(`FlumeConfluence: already closed: ${id}`)
    if (this.groups.has(id)) return new FlumeStartError(`FlumeConfluence: id already added: ${id}`)

    const group: Group = { running: null, opening: null, closing: null }
    this.groups.set(id, group)
    const result = await this.startGroup(id, group, sources)
    if (result instanceof Error && !group.closing) this.groups.delete(id)
    return result instanceof Error ? result : null
  }

  /** 起動失敗時は旧接続を維持する。同じグループの並行 replace は拒否する。 */
  async replace(
    id: string,
    sources: ReadonlyArray<FlumeSource>,
    options?: { readonly replaceTimeoutMs?: number },
  ): Promise<FlumeConfluenceReplacement | Error> {
    if (this.props instanceof Error) return this.props
    if (this.isClosed) return new FlumeStartError(`FlumeConfluence: already closed: ${id}`)
    const group = this.groups.get(id)
    if (!group?.running || group.closing)
      return new FlumeStartError(`FlumeConfluence: id not running: ${id}`)
    if (group.opening)
      return new FlumeStartError(`FlumeConfluence: replacement already pending: ${id}`)

    const selected = captureConfiguration("FlumeConfluence.replace", () => ({
      timeoutMs: options?.replaceTimeoutMs,
    }))
    if (selected instanceof Error) return selected
    const requested = selected.timeoutMs
    const timeoutMs =
      typeof requested === "number" && Number.isFinite(requested) && requested > 0
        ? Math.min(requested, 2_147_483_647)
        : DEFAULT_REPLACE_TIMEOUT_MS
    const result = await this.startGroup(id, group, sources, timeoutMs)
    if (result instanceof Error) return result
    // 稼働中グループのみ交換を開始するため、成功時は必ず旧接続の終了結果がある。
    return Object.freeze({ previous: result! })
  }

  remove(id: string): Promise<FlumeConfluenceRemoval | undefined> {
    const group = this.groups.get(id)
    if (!group) return Promise.resolve(undefined)
    if (group.closing) return group.closing

    // user callback が remove を再入しても同じ完了を待てるよう、副作用より先に登録する。
    group.closing = Promise.resolve().then(async () => {
      const opening = group.opening
      if (opening) attempt(() => opening.controller.abort())
      const [openingError, closed] = await Promise.all([opening?.done, group.running?.close()])
      this.groups.delete(id)
      return Object.freeze({
        groupId: id,
        closed: closed ?? null,
        openingError: openingError ?? null,
      })
    })
    return group.closing
  }

  closeAll(): Promise<ReadonlyArray<FlumeConfluenceRemoval>> {
    if (!this.closePromise) {
      this.closePromise = Promise.resolve().then(async () => {
        const results = await Promise.all([...this.groups.keys()].map((id) => this.remove(id)))
        return Object.freeze(
          results.filter((result): result is FlumeConfluenceRemoval => result !== undefined),
        )
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
  ): Promise<FlumeClosed | Error | null> {
    const props = this.props
    if (props instanceof Error) return props
    const completion = Promise.withResolvers<Error | null>()
    const opening: Opening = {
      controller: new AbortController(),
      done: completion.promise,
      finish: completion.resolve,
    }
    group.opening = opening
    const result = await attempt(async () => {
      const next = await new Flume({
        sources,
        onEvent: this.wrapOnEvent(id, props.onEvent),
        onError: this.wrapOnError(id, props.onError),
        callbackBuffer: props.callbackBuffer,
        deps: this.deps,
        reconnect: props.reconnect,
        signal: opening.controller.signal,
        openTimeoutMs: timeoutMs,
      }).open()
      if (next instanceof Error) return next
      if (this.isClosed || group.closing) {
        const closed = await next.close()
        return new FlumeStartError(`FlumeConfluence: removed during open: ${id}`, {
          errorCode: "FLUME_ABORTED",
          cause: closed.errors().length
            ? new AggregateError(
                closed.errors().map((entry) => entry.error),
                "cancelled startup cleanup failed",
              )
            : undefined,
        })
      }
      const previous = group.running
      group.running = next
      return previous ? await previous.close() : null
    })
    group.opening = null
    opening.finish(result instanceof Error ? result : null)
    return result
  }

  private wrapOnEvent(
    id: string,
    onEvent: FlumeConfluenceItemHandler | undefined,
  ): FlumeStreamHandler | undefined {
    if (!onEvent) return undefined

    return (item: FlumeStreamItem) => {
      const stamped: FlumeConfluenceItem = Object.freeze({ ...item, groupId: id })
      return onEvent(stamped)
    }
  }

  private wrapOnError(
    id: string,
    onError: FlumeErrorHandler | undefined,
  ): FlumeErrorHandler | undefined {
    if (!onError) return undefined

    return (log) => onError(snapshotValue({ ...log, detail: { ...log.detail, groupId: id } }))
  }
}
