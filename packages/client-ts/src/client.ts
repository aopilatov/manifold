// SocketClient — клиентский SDK. Транспорт-агностичен (WS/SSE), корреляция Command↔Reply по id,
// реестр подписок (источник истины), реконнект с джиттером, восстановление подписок, recovery,
// refresh токена (вариант B), ping.

import { create } from "@bufbuild/protobuf";
import {
  CommandSchema,
  type ConnectResult,
  type Error as ProtoError,
  type Publication,
  type Push,
  type Reply,
  type StreamPosition,
  type SubscribeResult,
} from "@socket/proto-gen";
import { jitteredDelay } from "./backoff.js";
import { encodeCommand } from "./codec.js";
import { makeTransport, type Transport } from "./transport.js";

export interface SocketOptions {
  url: string;
  getToken: () => Promise<string>;
  getSubToken?: (channel: string) => Promise<string>;
  /** Транспорт: ws (дефолт) | sse (фолбэк). */
  transport?: "ws" | "sse";
  requestTimeout?: number;
}

type Handler = (payload: any) => void;
type Pending = { resolve: (v: any) => void; reject: (e: any) => void; timer: any };

class Emitter {
  private handlers = new Map<string, Handler[]>();
  on(event: string, h: Handler): void {
    const arr = this.handlers.get(event) ?? [];
    arr.push(h);
    this.handlers.set(event, arr);
  }
  emit(event: string, payload?: any): void {
    for (const h of this.handlers.get(event) ?? []) h(payload);
  }
}

export class Subscription extends Emitter {
  position?: StreamPosition;
  subToken?: string;

  constructor(private client: SocketClient, readonly channel: string) {
    super();
  }

  async subscribe(): Promise<void> {
    if (this.client.options.getSubToken) {
      this.subToken = await this.client.options.getSubToken(this.channel);
    }
    const res = (await this.client._send({
      case: "subscribe",
      value: { channel: this.channel, recover: !!this.position, position: this.position, token: this.subToken ?? "" },
    })) as SubscribeResult;
    this.applySubscribeResult(res);
    this.emit("subscribed", res);
  }

  applySubscribeResult(res: SubscribeResult): void {
    if (res.position) this.position = res.position;
    for (const pub of res.publications) this.deliver(pub);
  }

  async unsubscribe(): Promise<void> {
    await this.client._send({ case: "unsubscribe", value: { channel: this.channel } });
    this.emit("unsubscribed");
  }

  async publish(data: Uint8Array, transient = false): Promise<void> {
    await this.client._send({ case: "publish", value: { channel: this.channel, data, transient } });
  }

  async presence(): Promise<Record<string, any>> {
    const res: any = await this.client._send({ case: "presence", value: { channel: this.channel } });
    return res.presence ?? {};
  }

  deliver(pub: Publication): void {
    if (this.position && pub.offset > 0n) this.position = { ...this.position, offset: pub.offset };
    this.emit("publication", pub);
  }
}

export class SocketClient extends Emitter {
  readonly options: SocketOptions;
  private transport?: Transport;
  private token?: string;
  private nextId = 1;
  private pending = new Map<number, Pending>();
  private subs = new Map<string, Subscription>();
  private attempt = 0;
  private explicitClose = false;
  private pingTimer: any;
  private refreshTimer: any;

  constructor(opts: SocketOptions) {
    super();
    this.options = opts;
  }

  newSubscription(channel: string): Subscription {
    let sub = this.subs.get(channel);
    if (!sub) {
      sub = new Subscription(this, channel);
      this.subs.set(channel, sub);
    }
    return sub;
  }

  async connect(): Promise<ConnectResult> {
    this.explicitClose = false;
    return this.open();
  }

  disconnect(): void {
    this.explicitClose = true;
    this.clearTimers();
    this.transport?.close();
  }

  private async open(): Promise<ConnectResult> {
    this.token = await this.options.getToken();
    const transport = makeTransport(this.options.transport ?? "ws", this.options.url);
    this.transport = transport;
    transport.onReply((r) => this.onReply(r));
    transport.onClose(() => this.onClose());

    const established = await transport.open(this.token);

    let res: ConnectResult;
    if (established) {
      // SSE: коннект выполнил GET; подписки восстанавливаем индивидуально
      res = established;
      for (const sub of this.subs.values()) this.resubscribe(sub);
    } else {
      // WS: шлём Connect-команду с батч-восстановлением подписок (1 RTT)
      const subsInit: Record<string, any> = {};
      for (const [ch, sub] of this.subs) {
        subsInit[ch] = { channel: ch, recover: !!sub.position, position: sub.position, token: sub.subToken ?? "" };
      }
      res = (await this._send({ case: "connect", value: { token: this.token, subs: subsInit, protocolVersion: 1 } })) as ConnectResult;
      for (const [ch, subRes] of Object.entries(res.subs ?? {})) {
        this.subs.get(ch)?.applySubscribeResult(subRes as SubscribeResult);
      }
    }

    this.attempt = 0;
    this.schedulePing(res.pingIntervalMs);
    this.scheduleRefresh(res.expiresInS);
    this.emit("connected", res);
    return res;
  }

  private resubscribe(sub: Subscription): void {
    this._send({
      case: "subscribe",
      value: { channel: sub.channel, recover: !!sub.position, position: sub.position, token: sub.subToken ?? "" },
    })
      .then((res) => sub.applySubscribeResult(res as SubscribeResult))
      .catch(() => {});
  }

  _send(method: any): Promise<any> {
    const id = this.nextId++;
    const cmd = create(CommandSchema, { id, method });
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error("request timeout"));
      }, this.options.requestTimeout ?? 10_000);
      this.pending.set(id, { resolve, reject, timer });
      try {
        if (!this.transport) throw new Error("not connected");
        this.transport.send(encodeCommand(cmd));
      } catch (e) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(e);
      }
    });
  }

  private onReply(reply: Reply): void {
    if (reply.id !== 0) {
      const p = this.pending.get(reply.id);
      if (!p) return;
      this.pending.delete(reply.id);
      clearTimeout(p.timer);
      if (reply.error) p.reject(asError(reply.error));
      else p.resolve(reply.payload?.value);
      return;
    }
    if (reply.payload?.case === "push") this.routePush(reply.payload.value as Push);
  }

  private routePush(push: Push): void {
    const sub = this.subs.get(push.channel);
    const ev = push.event;
    if (!ev) return;
    switch (ev.case) {
      case "pub":
        sub?.deliver(ev.value as Publication);
        break;
      case "join":
        sub?.emit("join", ev.value);
        break;
      case "leave":
        sub?.emit("leave", ev.value);
        break;
      case "unsubscribe":
        sub?.emit("unsubscribed", ev.value);
        break;
      case "disconnect":
        this.transport?.close();
        break;
      // неизвестные варианты безопасно игнорируются (версионирование)
    }
  }

  private onClose(): void {
    this.clearTimers();
    this.failPending();
    this.emit("disconnected");
    if (this.explicitClose) return;
    const delay = jitteredDelay(this.attempt++);
    setTimeout(() => this.open().catch(() => {}), delay);
  }

  private schedulePing(intervalMs: number): void {
    if (!intervalMs) return;
    this.pingTimer = setInterval(() => {
      this._send({ case: "ping", value: {} }).catch(() => this.transport?.close());
    }, intervalMs);
  }

  private scheduleRefresh(expiresInS: number): void {
    if (!expiresInS) return;
    const ms = Math.max(1_000, expiresInS * 1000 * 0.8);
    this.refreshTimer = setTimeout(async () => {
      try {
        this.token = await this.options.getToken();
        await this._send({ case: "refresh", value: { token: this.token } });
      } catch {
        this.transport?.close();
      }
    }, ms);
  }

  private clearTimers(): void {
    clearInterval(this.pingTimer);
    clearTimeout(this.refreshTimer);
  }

  private failPending(): void {
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new Error("connection closed"));
    }
    this.pending.clear();
  }
}

function asError(e: ProtoError): Error & { code?: number; temporary?: boolean } {
  const err = new Error(e.message || `error ${e.code}`) as any;
  err.code = e.code;
  err.temporary = e.temporary;
  return err;
}
