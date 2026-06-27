// Клиентский SDK (скелет). См. docs/architecture.md, раздел 11.
//
// Несёт всю клиентскую логику, чтобы прикладной код работал на высоком уровне:
//   const client = new SocketClient({ url, getToken });
//   const sub = client.newSubscription("chat:room:42");
//   sub.on("publication", (p) => ...);
//   await sub.subscribe();

export interface SocketOptions {
  url: string;
  /** Колбэк за connection JWT (источник — внешний бэкенд). */
  getToken: () => Promise<string>;
  /** Колбэк за sub-токеном приватного канала. */
  getSubToken?: (channel: string) => Promise<string>;
  /** Транспорт: ws (дефолт) с авто-фолбэком на sse. */
  transport?: "ws" | "sse" | "auto";
}

export type StreamPosition = { offset: bigint; epoch: string };

export interface SubscriptionState {
  channel: string;
  lastPosition?: StreamPosition;
  subToken?: string;
}

type Handler = (payload: unknown) => void;

export class SocketClient {
  private opts: SocketOptions;
  private token?: string;
  /** Реестр подписок — источник истины для реконнекта (раздел 7.11). */
  private subs = new Map<string, SubscriptionState>();

  constructor(opts: SocketOptions) {
    this.opts = opts;
  }

  async connect(): Promise<void> {
    this.token = await this.opts.getToken();
    // TODO(impl): открыть транспорт (WS/SSE), послать ConnectRequest с subs (1 RTT),
    //             разобрать ConnectResult, запустить ping/pong, повесить реконнект с jitter.
    throw new Error("not implemented");
  }

  newSubscription(channel: string): Subscription {
    const state: SubscriptionState = { channel };
    this.subs.set(channel, state);
    return new Subscription(this, state);
  }

  // TODO(impl):
  //  - reconnect(): jittered backoff, переиспользовать кэшированный JWT,
  //    восстановить все subs за 1 RTT через ConnectRequest.subs, recovery по offset/epoch.
  //  - refresh(): RefreshRequest при близком exp (вариант B), без реконнекта.
  //  - publish/presence/history, encode/decode protobuf (@socket/proto-gen).
  //  - безопасно скипать неизвестные Push-варианты (версионирование).
}

export class Subscription {
  private handlers = new Map<string, Handler[]>();
  constructor(private client: SocketClient, private state: SubscriptionState) {}

  on(event: "publication" | "join" | "leave" | "subscribed", h: Handler): this {
    const arr = this.handlers.get(event) ?? [];
    arr.push(h);
    this.handlers.set(event, arr);
    return this;
  }

  async subscribe(): Promise<void> {
    // TODO(impl): SubscribeRequest{ recover, position }, обработать recovered/publications.
    throw new Error("not implemented");
  }

  async unsubscribe(): Promise<void> {
    // TODO(impl): UnsubscribeRequest.
    throw new Error("not implemented");
  }
}
