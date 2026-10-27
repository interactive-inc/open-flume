/** 秘密値や本文を含めず、失敗した IO の段階を示す。 */
export type FlumeErrorContext = Readonly<{
  operation: string
  phase: string
  page?: number
}>
