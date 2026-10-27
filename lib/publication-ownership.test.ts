import { describe, expect, it } from "vitest"
import { Flume } from "@/flume"
import { FlumeSource } from "@/flume-source"
import type {
  FlumeCustomEvent,
  FlumeEvent,
  FlumeLog,
  FlumeSourceStartContext,
  FlumeStreamItem,
} from "@/types"

class Source extends FlumeSource {
  readonly name = "probe"
  protected async connect(_ctx: FlumeSourceStartContext): Promise<null> {
    return null
  }
  protected disconnect(): void {}
  receive(event: FlumeEvent): void {
    this.emit(event)
  }
  report(error: Error): void {
    this.context!.log.error({
      action: "probe.failure",
      message: "original",
      errorCode: "FLUME_CONNECTION_FAILED",
      error,
      detail: { phase: "connect", nested: { status: "original" } },
    })
  }
}
const event = (): FlumeCustomEvent => ({
  source: "custom",
  sourceName: "probe",
  type: "original",
  receivedAt: 1,
  data: { nested: { value: 1 } },
  meta: { label: "original" },
})

describe("publication ownership", () => {
  it("diagnoses a rejected event and continues with later events", async () => {
    const source = new Source()
    const received: FlumeEvent[] = []
    const running = await new Flume({
      sources: [source],
      onEvent: (item) => {
        if (item.kind === "event") received.push(item.event)
      },
    }).open()
    if (running instanceof Error) throw running
    const failure = new Error("payload getter failed")
    const input = event()
    Object.defineProperty(input.data, "broken", {
      enumerable: true,
      get() {
        throw failure
      },
    })
    source.receive(input)
    source.receive(event())
    await running.close()
    await running.drain()
    expect(received).toEqual([event()])
    expect(source.deliveryStatus()).toMatchObject({ rejectedEvents: 1, droppedEvents: 0 })
    expect(running.diagnostics().entries).toContainEqual(
      expect.objectContaining({
        action: "source.event.snapshot.error",
        errorCode: "FLUME_RUNTIME_FAILED",
        error: failure,
      }),
    )
  })

  it("isolates stream option getters and reports the chosen fallback", async () => {
    const running = await new Flume({ sources: [] }).open()
    if (running instanceof Error) throw running
    const failure = new Error("buffer getter failed")
    const stream = running.stream({
      get buffer(): number {
        throw failure
      },
    })
    expect(running.diagnostics().entries).toContainEqual(
      expect.objectContaining({
        action: "stream.options.error",
        errorCode: "FLUME_RUNTIME_FAILED",
        error: failure,
      }),
    )
    await stream.return!()
    await running.close()
  })

  it("keeps push, error, pull and diagnostic views consistent when an observer tries to rewrite a log", async () => {
    const source = new Source()
    const peers: FlumeLog[] = []
    const failure = new Error("original failure")
    const running = await new Flume({
      sources: [source],
      onEvent: (item) => {
        if (item.kind !== "log" || item.log.action !== "probe.failure") return
        Reflect.set(item.log, "level", "info")
        Reflect.set(item.log, "message", "changed by observer")
        Reflect.set(item.log.detail!, "phase", "changed by observer")
        Reflect.set(item.log.detail!.nested as object, "status", "changed by observer")
      },
      onError: (log) => peers.push(log),
    }).open()
    if (running instanceof Error) throw running
    const stream = running.stream()
    source.report(failure)
    await running.close()
    await running.drain()
    const pulled: FlumeStreamItem[] = []
    for await (const item of stream) pulled.push(item)
    const original = {
      level: "error",
      message: "original",
      error: failure,
      detail: { phase: "connect", nested: { status: "original" } },
    }
    expect(peers.find((log) => log.action === "probe.failure")).toMatchObject(original)
    expect(
      pulled.find((item) => item.kind === "log" && item.log.action === "probe.failure"),
    ).toMatchObject({ kind: "log", log: original })
    expect(
      running.diagnostics().entries.find((log) => log.action === "probe.failure"),
    ).toMatchObject(original)
  })

  it("captures the producer's event once and isolates observers without freezing the producer's object", async () => {
    const source = new Source()
    const received: FlumeEvent[] = []
    const input = event()
    const running = await new Flume({
      sources: [source],
      onEvent: (item) => {
        if (item.kind !== "event") return
        received.push(item.event)
        Reflect.set(item.event, "type", "observer")
        Reflect.set(item.event.meta, "label", "observer")
        Reflect.set((item.event.data as Record<string, unknown>).nested as object, "value", 3)
      },
    }).open()
    if (running instanceof Error) throw running
    const stream = running.stream()
    source.receive(input)
    Reflect.set(input, "type", "producer")
    Reflect.set(input.meta, "label", "producer")
    Reflect.set(input.data.nested as object, "value", 2)
    await running.close()
    await running.drain()
    const pulled: FlumeEvent[] = []
    for await (const item of stream) if (item.kind === "event") pulled.push(item.event)
    expect(received).toEqual([event()])
    expect(pulled).toEqual([event()])
    expect(input).toMatchObject({
      type: "producer",
      meta: { label: "producer" },
      data: { nested: { value: 2 } },
    })
  })
})
