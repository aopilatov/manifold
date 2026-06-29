// E2E of the SSE transport: same scenario, but over EventSource + POST (not WebSocket).
import crypto from "node:crypto";
import { EventSource } from "undici"; // EventSource is global in the browser; in Node it's a polyfill
globalThis.EventSource ??= EventSource;
import { SocketClient } from "../dist/index.js";

const SECRET = "dev-secret";
const WS_URL = "ws://127.0.0.1:18000/connection/websocket"; // the SDK derives the SSE URL itself

const b64url = (s) => Buffer.from(s).toString("base64url");
function mintJwt(payload, secret) {
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const data = `${head}.${body}`;
  return `${data}.${crypto.createHmac("sha256", secret).update(data).digest("base64url")}`;
}
const token = mintJwt(
  { sub: "u-sse", aud: "socket", channels: [{ match: "chat:room:*", allow: ["sub", "pub", "presence"] }] },
  SECRET,
);
const fail = (m) => {
  console.error("E2E SSE FAIL:", m);
  process.exit(1);
};

if (typeof EventSource === "undefined") fail("this Node has no global EventSource");

const client = new SocketClient({ url: WS_URL, transport: "sse", getToken: async () => token });

const res = await client.connect().catch((e) => fail("connect: " + e.message));
console.log("SSE connected, session:", res.client);

const sub = client.newSubscription("chat:room:1");
const received = new Promise((resolve) =>
  sub.on("publication", (p) => resolve(new TextDecoder().decode(p.data))),
);
await sub.subscribe().catch((e) => fail("subscribe: " + e.message));
console.log("subscribed over SSE");

await sub.publish(new TextEncoder().encode("hello-sse")).catch((e) => fail("publish: " + e.message));

const msg = await Promise.race([
  received,
  new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 5000)),
]).catch((e) => fail(e.message));

console.log("received over SSE:", msg);
if (msg !== "hello-sse") fail("payload mismatch");

client.disconnect();
console.log("E2E SSE OK");
process.exit(0);
