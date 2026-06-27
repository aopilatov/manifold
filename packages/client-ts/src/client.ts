// SocketClient — клиентский SDK (этап 2). Транспорт WebSocket, корреляция Command↔Reply по id,
// реестр подписок (источник истины), реконнект с джиттером, восстановление подписок за 1 RTT,
// recovery по offset/epoch, refresh токена (вариант B), ping.

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
import { decodeReply, encodeCommand } from "./codec.js";

export interface SocketOptions {
  url: string;
  /** Колбэк за connection JWT (источник — внешний бэкенд). */
  getToken: () => Promise<string>;
  /** Колбэк за sub-токеном приватного канала. */
  getSubToken?: (channel: string) => Promise<string>;
  /** Таймаут ожидания ответа на команду, мс. */
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
  /** Последняя позиция в потоке — для recovery при реконнекте. */
  position?: StreamPosition;
  subToken?: string;
  private subscribed = false;

  constructor(private client: SocketClient, readonly channel: string) {
    super();
  }

  async subscribe(): Promise<void> {
    if (this.client.options.getSubToken) {
      this.subToken = await this.client.options.getSubToken(this.channel);
    }
    const res = (await this.client._send({
      case: "subscribe",
      value: {
        channel: this.channel,
        recover: !!this.position,
        position: this.position,
        token: this.subToken ?? "",
      },
    })) as SubscribeResult;
    this.applySubscribeResult(res);
    this.subscribed = true;
    this.emit("subscribed", res);
  }

  applySubscribeResult(res: SubscribeResult): void {
    if (res.position) this.position = res.position;
    // догон пропущенного (recovery)
    for (const pub of res.publications) this.deliver(pub);
  }

  async unsubscribe(): Promise<void> {
    await this.client._send({ case: "unsubscribe", value: { channel: this.channel } });
    this.subscribed = false;
    this.emit("unsubscribed");
  }

  async publish(data: Uint8Array, transient = false): Promise<void> {
    await this.client._send({
      case: "publish",
      value: { channel: this.channel, data, transient },
    });
  }

  async presence(): Promise<Record<string, any>> {
    const res: any = await this.client._send({ case: "presence", value: { channel: this.channel } });
    return res.presence ?? {};
  }

  /** Доставить публикацию подписчику + обновить позицию (для recovery). */
  deliver(pub: Publication): void {
    if (this.position && pub.offset > 0n) this.position = { ...this.position, offset: pub.offset };
    this.emit("publication", pub);
  }
}

type ClientState = "disconnected" | "connecting" | "connected";

export class SocketClient extends Emitter {
  readonly options: SocketOptions;
  private ws?: WebSocket;
  private token?: string;
  private nextId = 1;
  private pending = new Map<number, Pending>();
  /** Реестр подписок — источник истины для реконнекта. */
  private subs = new Map<string, Subscription>();
  private state: ClientState = "disconnected";
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
    this.ws?.close();
    this.state = "disconnected";
  }

  // --- внутреннее ---

  private async open(): Promise<ConnectResult> {
    this.state = "connecting";
    this.token = await this.options.getToken();

    const ws = new WebSocket(this.options.url, "socket.v1");
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = (e) => reject(e);
    });
    ws.onmessage = (ev) => this.onMessage(ev as MessageEvent);
    ws.onclose = () => this.onClose();

    // восстановление всех подписок за 1 RTT
    const subsInit: Record<string, any> = {};
    for (const [ch, sub] of this.subs) {
      subsInit[ch] = {
        channel: ch,
        recover: !!sub.position,
        position: sub.position,
        token: sub.subToken ?? "",
      };
    }

    const res = (await this._send({
      case: "connect",
      value: { token: this.token, subs: subsInit, protocolVersion: 1 },
    })) as ConnectResult;

    // применить результаты восстановленных подписок
    for (const [ch, subRes] of Object.entries(res.subs ?? {})) {
      this.subs.get(ch)?.applySubscribeResult(subRes as SubscribeResult);
    }

    this.state = "connected";
    this.attempt = 0;
    this.schedulePing(res.pingIntervalMs);
    this.scheduleRefresh(res.expiresInS);
    this.emit("connected", res);
    return res;
  }

  /** Отправить команду и дождаться Reply (корреляция по id). */
  _send(method: any): Promise<any> {
    const id = this.nextId++;
    const cmd = create(CommandSchema, { id, method });
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error("not connected"));
        return;
      }
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error("request timeout"));
      }, this.options.requestTimeout ?? 10_000);
      this.pending.set(id, { resolve, reject, timer });
      this.ws.send(encodeCommand(cmd));
    });
  }

  private onMessage(ev: MessageEvent): void {
    const reply = decodeReply(new Uint8Array(ev.data as ArrayBuffer));
    if (reply.id !== 0) {
      this.resolvePending(reply);
      return;
    }
    if (reply.payload?.case === "push") this.routePush(reply.payload.value as Push);
  }

  private resolvePending(reply: Reply): void {
    const p = this.pending.get(reply.id);
    if (!p) return;
    this.pending.delete(reply.id);
    clearTimeout(p.timer);
    if (reply.error) p.reject(asError(reply.error));
    else p.resolve(reply.payload?.value);
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
        // сервер закрывает соединение; reconnect решит onClose по флагу
        this.ws?.close();
        break;
    }
  }

  private onClose(): void {
    this.clearTimers();
    this.failPending();
    this.state = "disconnected";
    this.emit("disconnected");
    if (this.explicitClose) return;
    const delay = jitteredDelay(this.attempt++);
    setTimeout(() => {
      this.open().catch(() => {
        /* следующий onClose снова запланирует реконнект */
      });
    }, delay);
  }

  private schedulePing(intervalMs: number): void {
    if (!intervalMs) return;
    this.pingTimer = setInterval(() => {
      this._send({ case: "ping", value: {} }).catch(() => this.ws?.close());
    }, intervalMs);
  }

  private scheduleRefresh(expiresInS: number): void {
    if (!expiresInS) return;
    const ms = Math.max(1_000, expiresInS * 1000 * 0.8); // обновить заранее
    this.refreshTimer = setTimeout(async () => {
      try {
        this.token = await this.options.getToken();
        await this._send({ case: "refresh", value: { token: this.token } });
      } catch {
        this.ws?.close();
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
