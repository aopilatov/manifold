# manifold-client

TypeScript client SDK for the [Manifold](https://hub.docker.com/r/saxikopilatov/manifold) realtime
engine — a configurable WebSocket pub/sub server ("like Centrifugo, but more configurable").

Handles the wire protocol (Protobuf), connection lifecycle, **reconnect with jittered backoff**,
**subscription restore in one round-trip**, **lossless recovery** (offset/epoch), token refresh,
and presence — over **WebSocket** or an **SSE** fallback.

```bash
npm install manifold-client
```

## Quick start

```ts
import { ManifoldClient } from "manifold-client";

const client = new ManifoldClient({
  url: "ws://localhost:8000/connection/websocket",
  // Your backend issues a connection JWT (HS256/RSA) — the SDK never mints tokens.
  getToken: async () => fetchTokenFromYourBackend(),
});

client.on("connected", (res) => console.log("connected as", res.client));
client.on("disconnected", () => console.log("disconnected (will auto-reconnect)"));

await client.connect();

const room = client.newSubscription("chat:room:42");

room.on("publication", (pub) => {
  console.log("message:", new TextDecoder().decode(pub.data), "offset", pub.offset);
});

await room.subscribe();

await room.publish(new TextEncoder().encode("hello"));
```

## Concepts

- **Channels & namespaces** — a channel is a colon-separated name; the first segment is the
  *namespace* (`chat:room:42` → namespace `chat`). The server's namespace config decides whether a
  channel is public or requires a token, and whether it keeps history/presence.
- **Capability tokens** — the connection JWT (from your backend) carries the channels/patterns the
  user may use. Public channels need no per-channel token; private ones are gated by the JWT or a
  per-subscription token (`getSubToken`).
- **Recovery** — for channels with history, the SDK tracks each subscription's position
  (`offset`/`epoch`) and, on reconnect, asks the server to resend whatever was missed — no gaps.

## Connecting

```ts
const client = new ManifoldClient({
  url: "ws://host:8000/connection/websocket",
  getToken: async () => "<connection JWT>",
  getSubToken: async (channel) => "<subscription JWT>", // optional, for private channels
  transport: "ws",        // "ws" (default) | "sse"
  requestTimeout: 10_000, // ms, per command
});

const result = await client.connect(); // resolves with ConnectResult
client.disconnect();                    // stop and don't auto-reconnect
```

The SDK reconnects automatically on connection loss with **jittered exponential backoff**, reuses
the cached JWT, and restores all subscriptions (with recovery) in a single round-trip. Token
expiry is handled by calling `getToken()` again and refreshing over the live connection — no
reconnect.

## Subscriptions

```ts
const sub = client.newSubscription("news:sports");

sub.on("publication", (pub) => { /* pub.data: Uint8Array, pub.offset: bigint */ });
sub.on("join",  (info) => {});   // presence join (if enabled on the namespace)
sub.on("leave", (info) => {});
sub.on("subscribed",   (res) => {});
sub.on("unsubscribed", () => {});

await sub.subscribe();
await sub.publish(new TextEncoder().encode("payload"));      // if you have publish rights
await sub.publish(typingBytes, /* transient */ true);        // fire-and-forget, skips history
const online = await sub.presence();                          // { clientId: { user, client } }
await sub.unsubscribe();
```

`newSubscription` is idempotent per channel — calling it twice returns the same `Subscription`.

## Transports

- **WebSocket** (default) — one bidirectional binary socket.
- **SSE** (`transport: "sse"`) — fallback for networks that block WebSocket: an `EventSource`
  downstream + `POST` upstream. Same protocol, base64-framed.

## API

| Member | Description |
|---|---|
| `new ManifoldClient(options)` | Create a client. |
| `client.connect()` | Open the connection, return `ConnectResult`. |
| `client.disconnect()` | Close and stop auto-reconnect. |
| `client.newSubscription(channel)` | Get/create a `Subscription`. |
| `client.on("connected" \| "disconnected", cb)` | Connection lifecycle events. |
| `sub.subscribe()` / `sub.unsubscribe()` | Join / leave the channel. |
| `sub.publish(data, transient?)` | Publish bytes (transient = at-most-once, no history). |
| `sub.presence()` | Current online clients on the channel. |
| `sub.on(event, cb)` | `publication` · `join` · `leave` · `subscribed` · `unsubscribed`. |
| `sub.position` | Last `StreamPosition` (`offset`/`epoch`) — used for recovery. |

### `ManifoldOptions`

```ts
interface ManifoldOptions {
  url: string;                              // ws:// or wss:// connection URL
  getToken: () => Promise<string>;          // connection JWT from your backend
  getSubToken?: (channel: string) => Promise<string>; // per-channel JWT for private channels
  transport?: "ws" | "sse";                 // default "ws"
  requestTimeout?: number;                  // ms, default 10000
}
```

## Browser vs Node

- **Browser** — `WebSocket` and `EventSource` are global; nothing extra needed.
- **Node** — `WebSocket` is global on Node ≥ 22. For the **SSE** transport, polyfill `EventSource`:

  ```ts
  import { EventSource } from "undici";
  globalThis.EventSource ??= EventSource;
  ```

## Related

- Server image: [`saxikopilatov/manifold`](https://hub.docker.com/r/saxikopilatov/manifold)
- Generated protobuf types: [`manifold-proto-gen`](https://www.npmjs.com/package/manifold-proto-gen) (dependency of this SDK)

## License

MIT
