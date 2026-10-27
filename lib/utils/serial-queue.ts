import { attempt } from "@/utils/attempt"

type Task = { run: () => void | Promise<void>; finish: () => void }

/**
 * 投入順に task を直列実行する。同期転送は immediate でその場で処理できる。
 * task の例外で後続は止めない。所有者が容量と失敗診断を管理する。
 */
export class FlumeSerialQueue {
  private readonly tasks: Task[] = []
  private readonly drained: Array<() => void> = []
  private running = false

  constructor(private readonly options: { immediate?: boolean } = {}) {}

  get size(): number {
    return this.tasks.length + Number(this.running)
  }

  add(run: () => void | Promise<void>): Promise<void> {
    const completion = Promise.withResolvers<void>()
    this.tasks.push({ run, finish: completion.resolve })
    if (!this.running && this.tasks.length === 1) {
      if (this.options.immediate) this.pump()
      else void Promise.resolve().then(() => this.pump())
    }
    return completion.promise
  }

  drain(): Promise<void> {
    if (this.size === 0) return Promise.resolve()
    return new Promise((resolve) => this.drained.push(resolve))
  }

  private pump(): void {
    if (this.running) return
    let task: Task | undefined
    while ((task = this.tasks.shift())) {
      this.running = true
      const current = task
      const result = attempt(current.run)
      if (result instanceof Promise) {
        void result.then(() => {
          this.running = false
          current.finish()
          this.pump()
        })
        return
      }
      this.running = false
      current.finish()
    }
    for (const finish of this.drained.splice(0)) finish()
  }
}
