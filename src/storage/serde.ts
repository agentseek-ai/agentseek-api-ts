// Replicated from @langchain/langgraph-api src/utils/serde.mts (MIT) —
// not exported by the package, needed so persisted events match live SSE shape.
export const serialiseAsDict = (obj: unknown): string => {
  return JSON.stringify(obj, function (key: string | number, value: unknown) {
    const rawValue = (this as Record<string | number, unknown>)[key]
    if (
      rawValue != null &&
      typeof rawValue === 'object' &&
      'toDict' in rawValue &&
      typeof (rawValue as { toDict: unknown }).toDict === 'function'
    ) {
      const { type, data } = (rawValue as { toDict: () => { type: string; data: object } }).toDict()
      return { ...data, type }
    }
    return value
  })
}

export const serializeError = (error: unknown): { error: string; message: string } => {
  if (error instanceof Error) {
    return { error: error.name, message: error.message }
  }
  return { error: 'Error', message: JSON.stringify(error) }
}
