import type { FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import { FlumeLogger } from "@/logger"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeRandom } from "@/utils/safe-random"
import { FlumeConnectionError } from "@/errors/connection-error"

type Props = {
  onSend: () => void
  onZombie: () => void
  log: FlumeLogger
  deps: Pick<
    FlumeRuntimeDeps,
    "setInterval" | "clearInterval" | "setTimeout" | "clearTimeout" | "random"
  >
}

/**
 * Discord Gateway のハートビート。
 * 初回送信は `random()*heartbeat_interval` ms 後 (spec)、以降は heartbeat_interval ごと。
 * 前回送信に対する ACK が来ていなければ zombie 通知。すべての callback / timer body は throw を
 * 飲み込みログ記録する (host の timer queue に uncaught を漏らさない)
 */
export class FlumeDiscordHeartbeat {
  private initialTimer: FlumeTimerHandle | null = null

  private intervalTimer: FlumeTimerHandle | null = null

  private ackReceived = true

  private generation = 0

  private active = false

  constructor(private readonly props: Props) {}

  start(intervalMs: number): Error | null {
    this.stop()
    if (!Number.isInteger(intervalMs) || intervalMs < 1 || intervalMs > 2_147_483_647) {
      return new FlumeConnectionError(
        "heartbeat interval must be an integer from 1 to 2147483647ms",
        {
          errorCode: "FLUME_PROTOCOL_INVALID",
        },
      )
    }
    this.active = true
    this.ackReceived = true
    const scheduledGeneration = ++this.generation

    const initialDelay = safeRandom({ deps: this.props.deps }) * intervalMs

    const initialResult = attempt(() =>
      this.props.deps.setTimeout(() => {
        if (scheduledGeneration !== this.generation) return
        this.initialTimer = null
        this.safeFire()
        if (scheduledGeneration !== this.generation) return

        const intervalResult = attempt(() =>
          this.props.deps.setInterval(() => {
            if (scheduledGeneration === this.generation) this.safeFire()
          }, intervalMs),
        )
        if (intervalResult instanceof Error) {
          this.props.log.error({
            action: "heartbeat.interval.schedule.error",
            message: safeErrorMessage({ error: intervalResult }),
            error: intervalResult,
          })
          this.intervalTimer = null
          this.safeZombie()
        } else {
          this.intervalTimer = intervalResult
        }
      }, initialDelay),
    )
    if (initialResult instanceof Error) {
      this.props.log.error({
        action: "heartbeat.initial.schedule.error",
        message: safeErrorMessage({ error: initialResult }),
        error: initialResult,
      })
      this.initialTimer = null
      this.active = false
      return initialResult
    } else {
      this.initialTimer = initialResult
    }

    return null
  }

  stop(): void {
    this.active = false
    this.generation++

    if (this.initialTimer !== null) {
      const handle = this.initialTimer
      const r = attempt(() => this.props.deps.clearTimeout(handle))
      if (r instanceof Error) {
        this.props.log.error({
          action: "heartbeat.initial.clear.error",
          message: safeErrorMessage({ error: r }),
          error: r,
        })
      }
      this.initialTimer = null
    }

    if (this.intervalTimer !== null) {
      const handle = this.intervalTimer
      const r = attempt(() => this.props.deps.clearInterval(handle))
      if (r instanceof Error) {
        this.props.log.error({
          action: "heartbeat.interval.clear.error",
          message: safeErrorMessage({ error: r }),
          error: r,
        })
      }
      this.intervalTimer = null
    }
  }

  ack(): void {
    this.ackReceived = true
  }

  request(): void {
    if (!this.active) return
    // 追加要求への返答で、定期 heartbeat の ACK 待ち期限を早めたり延ばしたりしない。
    safeInvokeCallback({
      fn: this.props.onSend,
      onError: (error) =>
        this.props.log.error({
          action: "heartbeat.fire.error",
          message: safeErrorMessage({ error }),
          error,
        }),
    })
  }

  isRunning(): boolean {
    return this.initialTimer !== null || this.intervalTimer !== null
  }

  private safeFire(): void {
    safeInvokeCallback({
      fn: () => this.fire(),
      onError: (error) => {
        this.props.log.error({
          action: "heartbeat.fire.error",
          message: safeErrorMessage({ error }),
          error,
        })
      },
    })
  }

  private safeZombie(): void {
    safeInvokeCallback({
      fn: this.props.onZombie,
      onError: (error) => {
        this.props.log.error({
          action: "heartbeat.zombie.error",
          message: safeErrorMessage({ error }),
          error,
        })
      },
    })
  }

  private fire(): void {
    if (!this.ackReceived) {
      this.props.onZombie()
      return
    }
    this.ackReceived = false
    this.props.onSend()
  }
}
