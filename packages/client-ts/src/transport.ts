// SDK transports behind a single interface (architecture section 4).
// WS — bidirectional socket. SSE — split session (EventSource downstream + POST upstream).

import { type ConnectResult, type Reply } from "@socket/proto-gen";
import { decodeReply } from "./codec.js";

export interface Transport {
  /**
   * Open the transport. Returns ConnectResult if the transport performs the connect itself (SSE),
   * otherwise null — the client sends the Connect command itself (WS).
   */
  open(token: string): Promise<ConnectResult | null>;
  send(bytes: Uint8Array): void;
  onReply(cb: (reply: Reply) => void): void;
  onClose(cb: () => void): void;
  close(): void;
}

function fromBase64(s: string): Uint8Array {
  const g = globalThis as any;
  if (typeof g.Buffer !== "undefined") return new Uint8Array(g.Buffer.from(s, "base64"));
  const bin = atob(s);
  const arr = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
  return arr;
}

export class WsTransport implements Transport {
  private ws?: WebSocket;
  private replyCb: (r: Reply) => void = () => {};
  private closeCb: () => void = () => {};

  constructor(private url: string) {}

  async open(_token: string): Promise<null> {
    const ws = new WebSocket(this.url, "socket.v1");
    ws.binaryType = "arraybuffer";
    this.ws = ws;
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = (e) => reject(e);
    });
    ws.onmessage = (ev) => this.replyCb(decodeReply(new Uint8Array((ev as MessageEvent).data as ArrayBuffer)));
    ws.onclose = () => this.closeCb();
    return null; // WS: the client performs the connect via a Connect command
  }

  send(bytes: Uint8Array): void {
    this.ws?.send(bytes);
  }
  onReply(cb: (r: Reply) => void): void {
    this.replyCb = cb;
  }
  onClose(cb: () => void): void {
    this.closeCb = cb;
  }
  close(): void {
    this.ws?.close();
  }
}

export class SseTransport implements Transport {
  private es?: EventSource;
  private sessionId?: string;
  private replyCb: (r: Reply) => void = () => {};
  private closeCb: () => void = () => {};
  private sseUrl: string;
  private emitUrl: string;

  constructor(wsUrl: string) {
    // ws://host/connection/websocket → http://host/connection/sse(/emit)
    const base = wsUrl.replace(/^ws/, "http").replace(/\/connection\/websocket$/, "");
    this.sseUrl = `${base}/connection/sse`;
    this.emitUrl = `${base}/connection/sse/emit`;
  }

  open(token: string): Promise<ConnectResult> {
    return new Promise((resolve, reject) => {
      const es = new EventSource(`${this.sseUrl}?token=${encodeURIComponent(token)}`);
      this.es = es;
      let connected = false;
      es.onmessage = (ev) => {
        const reply = decodeReply(fromBase64((ev as MessageEvent).data as string));
        if (!connected) {
          connected = true;
          if (reply.payload?.case !== "connect") {
            reject(new Error("first SSE event is not a ConnectResult"));
            return;
          }
          this.sessionId = reply.payload.value.client;
          resolve(reply.payload.value);
          return;
        }
        this.replyCb(reply);
      };
      es.onerror = () => {
        if (!connected) reject(new Error("SSE: failed to open"));
        else {
          es.close(); // don't rely on EventSource auto-reconnect — the client reconnects
          this.closeCb();
        }
      };
    });
  }

  send(bytes: Uint8Array): void {
    // upstream — binary POST (no base64)
    void fetch(this.emitUrl, {
      method: "POST",
      headers: { "X-Session-Id": this.sessionId ?? "", "content-type": "application/octet-stream" },
      body: bytes as unknown as BodyInit,
    });
  }
  onReply(cb: (r: Reply) => void): void {
    this.replyCb = cb;
  }
  onClose(cb: () => void): void {
    this.closeCb = cb;
  }
  close(): void {
    this.es?.close();
  }
}

export function makeTransport(kind: "ws" | "sse", url: string): Transport {
  return kind === "sse" ? new SseTransport(url) : new WsTransport(url);
}
