import { z } from "zod/v4"

// s / t は nullish: Discord は op-11 ACK 等でフィールド自体を省略することがあり、
// nullable だと parse 失敗 → frame drop → 偽 zombie 判定に繋がる
export const FlumeGatewayMessageSchema = z.object({
  op: z.number(),
  d: z.unknown().optional(),
  s: z.number().nullish(),
  t: z.string().nullish(),
})
