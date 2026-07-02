import { describe, it, expect } from "vitest"
import { parseFlumeDiscordGatewayMessage } from "@/discord/parse-discord-gateway-message"
import { FlumeParseError } from "@/errors/parse-error"

describe("parseFlumeDiscordGatewayMessage", () => {
  it("valid JSON with op/d/s/t returns FlumeGatewayMessage", () => {
    const raw = JSON.stringify({ op: 0, d: { key: "value" }, s: 1, t: "MESSAGE_CREATE" })

    const message = parseFlumeDiscordGatewayMessage(raw)

    expect(message).not.toBeInstanceOf(FlumeParseError)
    if (message instanceof FlumeParseError) return
    expect(message.op).toBe(0)
    expect(message.d).toEqual({ key: "value" })
    expect(message.s).toBe(1)
    expect(message.t).toBe("MESSAGE_CREATE")
  })

  it("invalid JSON returns FlumeParseError", () => {
    const message = parseFlumeDiscordGatewayMessage("not json {{{")

    expect(message).toBeInstanceOf(FlumeParseError)
  })

  it("JSON missing op field returns FlumeParseError", () => {
    const raw = JSON.stringify({ d: null, s: null, t: null })

    const message = parseFlumeDiscordGatewayMessage(raw)

    expect(message).toBeInstanceOf(FlumeParseError)
  })

  it("null d field returns d as null in result", () => {
    const raw = JSON.stringify({ op: 1, d: null, s: null, t: null })

    const message = parseFlumeDiscordGatewayMessage(raw)

    expect(message).not.toBeInstanceOf(FlumeParseError)
    if (message instanceof FlumeParseError) return
    expect(message.d).toBe(null)
  })

  it("INVALID_SESSION with d=true is accepted (boolean d)", () => {
    const raw = JSON.stringify({ op: 9, d: true, s: null, t: null })

    const message = parseFlumeDiscordGatewayMessage(raw)

    expect(message).not.toBeInstanceOf(FlumeParseError)
    if (message instanceof FlumeParseError) return
    expect(message.d).toBe(true)
  })

  it("INVALID_SESSION with d=false is accepted (boolean d)", () => {
    const raw = JSON.stringify({ op: 9, d: false, s: null, t: null })

    const message = parseFlumeDiscordGatewayMessage(raw)

    expect(message).not.toBeInstanceOf(FlumeParseError)
    if (message instanceof FlumeParseError) return
    expect(message.d).toBe(false)
  })

  it("frame omitting s and t (bare op-11 ACK) is accepted and normalized to null", () => {
    const message = parseFlumeDiscordGatewayMessage('{"op":11}')

    expect(message).not.toBeInstanceOf(FlumeParseError)
    if (message instanceof FlumeParseError) return
    expect(message.op).toBe(11)
    expect(message.s).toBe(null)
    expect(message.t).toBe(null)
    expect(message.d).toBe(null)
  })

  it("frame with undefined-producing omissions keeps boolean d intact", () => {
    const message = parseFlumeDiscordGatewayMessage('{"op":9,"d":false}')

    expect(message).not.toBeInstanceOf(FlumeParseError)
    if (message instanceof FlumeParseError) return
    expect(message.d).toBe(false)
    expect(message.s).toBe(null)
    expect(message.t).toBe(null)
  })

  it("HEARTBEAT with numeric d (last seq) is accepted", () => {
    const raw = JSON.stringify({ op: 1, d: 42, s: null, t: null })

    const message = parseFlumeDiscordGatewayMessage(raw)

    expect(message).not.toBeInstanceOf(FlumeParseError)
    if (message instanceof FlumeParseError) return
    expect(message.d).toBe(42)
  })
})
