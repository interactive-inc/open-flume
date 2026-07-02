import type {
  FlumeConfluenceItem,
  FlumeConfluenceItemHandler,
  FlumeErrorHandler,
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
  deps?: FlumeRuntimeDeps
  reconnect?: boolean | FlumeReconnectOptions
}

/**
 * 複数の `Flume` を束ねて動的に増減させる上位レイヤー。各 Flume は immutable のまま、
 * `add()` で新しいグループを起動し `remove()` で個別に停止する。全グループの firehose は
 * `onEvent` 1 本に合流し、各 item には発信元グループ id が `groupId` としてスタンプされる。
 * Flume 本体の FSM / rollback / reconnect はそのまま再利用される。
 *
 * `replace(id, sources)` は同じ id のグループを差し替える。新グループを先に起動し、
 * 起動成功時にのみ旧グループを停止するので連続稼働を維持できる (token rotation 用途)。
 * 起動失敗時は旧グループはそのまま走り続ける。
 * 注意: 失敗した replace / add に渡した source インスタンスは consumed になるため、
 * リトライには新しいインスタンスを構築する必要がある。
 *
 * `closeAll()` は終端操作。以後の `add()` / `replace()` は拒否され、closeAll と並行して
 * 起動中だったグループも commit 時点で破棄される (シャットダウン後に誰にも止められない
 * グループが残らない)。
 *
 * throw しない流儀に従い `add()` / `replace()` は `Error | null` を返す
 */
export class FlumeConfluence {
  private readonly running = new Map<string, FlumeRunning>()

  /** open() を await 中でまだ Map に commit されていないグループの id (add 重複と remove 追跡用) */
  private readonly pendingIds = new Set<string>()

  private readonly removedWhilePending = new Set<string>()

  private isClosedFlag = false

  private readonly deps: FlumeRuntimeDeps

  constructor(private readonly props: Props = {}) {
    this.deps = props.deps ?? createFlumeDefaultDeps()
  }

  get isClosed(): boolean {
    return this.isClosedFlag
  }

  /** sources を 1 グループとして起動。id 重複や起動失敗は `Error` で返す (throw しない) */
  async add(id: string, sources: ReadonlyArray<FlumeSource>): Promise<Error | null> {
    if (this.isClosedFlag) {
      return new FlumeStartError(`FlumeConfluence: already closed: ${id}`)
    }

    if (this.running.has(id) || this.pendingIds.has(id)) {
      return new FlumeStartError(`FlumeConfluence: id already added: ${id}`)
    }

    this.pendingIds.add(id)

    try {
      const running = await this.openGroup(id, sources, undefined)
      if (running instanceof Error) return running

      // open() を await している間に closeAll / remove が走った可能性がある (TOCTOU)。
      // 後勝ちで Map を上書きすると前の FlumeRunning が宙に浮いて close されないので、
      // 開き直した方をその場で閉じて弾く
      if (this.isClosedFlag) {
        await running.close()
        return new FlumeStartError(`FlumeConfluence: closed during add: ${id}`)
      }

      if (this.removedWhilePending.has(id)) {
        await running.close()
        return new FlumeStartError(`FlumeConfluence: removed during add: ${id}`)
      }

      if (this.running.has(id)) {
        await running.close()
        return new FlumeStartError(`FlumeConfluence: id already added: ${id}`)
      }

      this.running.set(id, running)
      return null
    } finally {
      this.pendingIds.delete(id)
      this.removedWhilePending.delete(id)
    }
  }

  /**
   * 既存グループを新しい sources で差し替える。新グループを先に起動し、成功時のみ旧を停止する。
   * 起動失敗 / replaceTimeoutMs (既定 10s) 経過時は新グループを破棄し旧を走らせたまま返す。
   * 旧グループが存在しない場合は `Error` を返す (replace は add と違ってグループの存在を前提とする)
   */
  async replace(
    id: string,
    sources: ReadonlyArray<FlumeSource>,
    options?: { readonly replaceTimeoutMs?: number },
  ): Promise<Error | null> {
    if (this.isClosedFlag) {
      return new FlumeStartError(`FlumeConfluence: already closed: ${id}`)
    }

    const previous = this.running.get(id)
    if (!previous) {
      return new FlumeStartError(`FlumeConfluence: id not running: ${id}`)
    }

    const timeoutMs = options?.replaceTimeoutMs ?? DEFAULT_REPLACE_TIMEOUT_MS
    const next = await this.openGroup(id, sources, timeoutMs)
    if (next instanceof Error) return next

    if (this.isClosedFlag) {
      await next.close()
      return new FlumeStartError(`FlumeConfluence: closed during replace: ${id}`)
    }

    // 起動完了までの間に他者が remove / replace を確定した場合は新グループを破棄して降りる
    if (this.running.get(id) !== previous) {
      await next.close()
      return new FlumeStartError(`FlumeConfluence: ${id} concurrently mutated during replace`)
    }

    this.running.set(id, next)
    await previous.close()
    return null
  }

  /**
   * 指定グループだけ close。他グループは無停止。未知の id は no-op。
   * 起動中 (add が open を await 中) の id は commit 時点で破棄されるよう予約する
   */
  async remove(id: string): Promise<void> {
    if (this.pendingIds.has(id)) {
      this.removedWhilePending.add(id)
    }

    const running = this.running.get(id)
    if (!running) return

    this.running.delete(id)
    await running.close()
  }

  /** 終端操作。全グループを close し、以後の add / replace を拒否する */
  async closeAll(): Promise<void> {
    this.isClosedFlag = true

    const ids = [...this.running.keys()]
    await Promise.all(ids.map((id) => this.remove(id)))
  }

  has(id: string): boolean {
    return this.running.has(id)
  }

  ids(): ReadonlyArray<string> {
    return [...this.running.keys()]
  }

  /**
   * 1 グループ分の Flume を開いて FlumeRunning を返す。timeoutMs を指定すると AbortSignal を
   * ctx.signal として各 source へ注入し、超過時に abort して進行中の connect ごと中止する
   * (source 側は base クラスが signal を購読して stop() を発火する)。
   * 失敗時の rollback は Flume 本体に任せる
   */
  private async openGroup(
    id: string,
    sources: ReadonlyArray<FlumeSource>,
    timeoutMs: number | undefined,
  ): Promise<FlumeRunning | Error> {
    const controller = timeoutMs === undefined ? null : new AbortController()
    const timeoutHandle =
      controller === null
        ? null
        : this.deps.setTimeout(() => controller.abort(), timeoutMs ?? DEFAULT_REPLACE_TIMEOUT_MS)

    const flume = new Flume({
      sources,
      onEvent: this.wrapOnEvent(id),
      onError: this.props.onError,
      deps: this.props.deps,
      reconnect: this.props.reconnect,
      signal: controller?.signal,
    })

    const result = await flume.open()
    if (timeoutHandle !== null) {
      attempt(() => this.deps.clearTimeout(timeoutHandle))
    }

    if (result instanceof Error) {
      if (controller !== null && controller.signal.aborted) {
        return new FlumeStartError(
          `FlumeConfluence: open of "${id}" timed out after ${timeoutMs}ms`,
          { cause: result },
        )
      }
      return result
    }

    return result
  }

  private wrapOnEvent(id: string): FlumeStreamHandler | undefined {
    const onEvent = this.props.onEvent
    if (!onEvent) return undefined

    return (item: FlumeStreamItem) => {
      const stamped: FlumeConfluenceItem = { ...item, groupId: id }
      onEvent(stamped)
    }
  }
}
