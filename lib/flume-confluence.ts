import type {
  FlumeErrorHandler,
  FlumeReconnectOptions,
  FlumeRuntimeDeps,
  FlumeStreamHandler,
} from "@/types"
import type { FlumeSource } from "@/flume-source"
import { FlumeStartError } from "@/errors/start-error"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"

type Props = {
  /** 配下の全 Flume の firehose をここへ合流させる単一 sink */
  onEvent?: FlumeStreamHandler
  /** error レベル log だけ (全 Flume 共通) */
  onError?: FlumeErrorHandler
  deps?: FlumeRuntimeDeps
  reconnect?: FlumeReconnectOptions
}

/**
 * 複数の `Flume` を束ねて動的に増減させる上位レイヤー。各 Flume は immutable のまま、
 * `add()` で新しいグループを起動し `remove()` で個別に停止する。全グループの firehose は
 * `onEvent` 1 本に合流する。Flume 本体の FSM / rollback / reconnect はそのまま再利用される。
 *
 * id はグループの管理ハンドル (add/remove 用)。合流ストリーム自体は id を持たず、
 * item の中の source 名で発信元を判別する。throw しない流儀に従い `add()` は `Error | null` を返す
 */
export class FlumeConfluence {
  private readonly running = new Map<string, FlumeRunning>()

  constructor(private readonly props: Props = {}) {}

  /** sources を 1 グループとして起動。id 重複や起動失敗は `Error` で返す (throw しない) */
  async add(id: string, sources: ReadonlyArray<FlumeSource>): Promise<Error | null> {
    if (this.running.has(id)) {
      return new FlumeStartError(`FlumeConfluence: id already added: ${id}`)
    }

    const flume = new Flume({
      sources,
      onEvent: this.props.onEvent,
      onError: this.props.onError,
      deps: this.props.deps,
      reconnect: this.props.reconnect,
    })

    const running = await flume.open()
    if (running instanceof Error) return running

    this.running.set(id, running)
    return null
  }

  /** 指定グループだけ close。他グループは無停止。未知の id は no-op */
  async remove(id: string): Promise<void> {
    const running = this.running.get(id)
    if (!running) return

    this.running.delete(id)
    await running.close()
  }

  async closeAll(): Promise<void> {
    const ids = [...this.running.keys()]
    await Promise.all(ids.map((id) => this.remove(id)))
  }

  has(id: string): boolean {
    return this.running.has(id)
  }

  ids(): ReadonlyArray<string> {
    return [...this.running.keys()]
  }
}
