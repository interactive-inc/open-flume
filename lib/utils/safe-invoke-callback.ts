import { safeNormalizeError } from "@/utils/safe-normalize-error"

type Props = {
  fn: () => void
  onError: (error: Error) => void
}

/**
 * fire-and-forget でユーザーコールバックを呼び出す。sync throw と async reject のどちらも
 * `onError(Error)` に正規化して通知。`onError` 自身が throw しても外に漏らさない。
 * 戻り値を持たない fire-and-forget 専用のため log/出力先には依存しない (caller が onError で決める)
 */
export function safeInvokeCallback(props: Props): void {
  try {
    Promise.resolve(props.fn())
      .catch((err: unknown) => {
        try {
          props.onError(safeNormalizeError({ value: err }))
        } catch {
          // onError 自身が throw した場合の last-ditch guard
        }
      })
      .catch(() => {})
  } catch (err) {
    try {
      props.onError(safeNormalizeError({ value: err }))
    } catch {
      // 同上
    }
  }
}
