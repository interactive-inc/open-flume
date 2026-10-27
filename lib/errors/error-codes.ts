/** 公開診断コード。意味を変更・再利用しない。action が発生箇所、cause が元の例外を示す。 */
export const FLUME_ERROR_CODES = Object.freeze({
  FLUME_ABORTED: "中断を要求されました。signal と停止を要求した操作を確認してください。",
  FLUME_START_FAILED:
    "起動が完了しませんでした。cause と source 別の起動エラーを確認してください。",
  FLUME_SOURCE_REUSED: "使用済みの Source です。再起動には新しいインスタンスを作成してください。",
  FLUME_STOP_FAILED:
    "リソースの停止に失敗しました。close の errors() と cause を確認してください。",
  FLUME_CONNECTION_FAILED:
    "接続に失敗しました。切断コード、接続段階、ネットワークを確認してください。",
  FLUME_HTTP_FAILED: "HTTP 通信が失敗しました。status、API 固有の code、cause を確認してください。",
  FLUME_AUTH_FAILED: "接続先に認証・権限を拒否されました。資格情報と要求権限を確認してください。",
  FLUME_RATE_LIMITED:
    "接続先に待機を要求されました。retryAfterMs またはログの待機時間を確認してください。",
  FLUME_TIMEOUT:
    "操作の期限を超えました。context の操作・段階と、通信先や保存先の応答を確認してください。",
  FLUME_PARSE_FAILED: "接続先の応答を解釈できませんでした。action と検証エラーを確認してください。",
  FLUME_PROTOCOL_INVALID:
    "接続先の応答がプロトコルに違反しています。action と検証結果を確認してください。",
  FLUME_CALLBACK_FAILED:
    "コールバックが失敗しました。detail の callback と元の error を確認してください。",
  FLUME_BUFFER_OVERFLOW:
    "配送上限を超え、通知を破棄しました。処理速度と buffer 設定を確認してください。",
  FLUME_TIMER_FAILED:
    "タイマーの予約または取消しが失敗しました。注入した runtime と cause を確認してください。",
  FLUME_RUNTIME_FAILED: "runtime の依存操作が失敗しました。action と cause を確認してください。",
  FLUME_RECONNECT_EXHAUSTED:
    "再接続回数が上限に達しました。直前の接続エラーと reconnect 設定を確認してください。",
  FLUME_STATE_FAILED:
    "スケジュール状態の読取または保存が失敗しました。statePersister と cause を確認してください。",
  FLUME_CRON_INVALID: "スケジュールを計算できませんでした。cron 式と対象時刻を確認してください。",
  FLUME_INTERNAL_ERROR:
    "内部処理の想定外の失敗です。action、source、cause を添えて報告してください。",
  FLUME_EXTERNAL_ERROR: "カスタム処理の診断です。action、source、元の error を確認してください。",
})

export type FlumeErrorCode = keyof typeof FLUME_ERROR_CODES
