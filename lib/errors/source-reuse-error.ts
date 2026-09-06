import { FlumeStartError } from "@/errors/start-error"

/** 起動を取得する前の拒否。Flume は他の起動処理が所有する Source を rollback しない。 */
export class FlumeSourceReuseError extends FlumeStartError {}
