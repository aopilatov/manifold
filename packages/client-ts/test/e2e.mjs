// E2E: SDK ↔ реальный Rust-сервер по WebSocket.
// Запускается при поднятом socket-server (smoke-конфиг, порт 18000, secret dev-secret).
import crypto from "node:crypto";
import { SocketClient } from "../dist/index.js";

const SECRET = "dev-secret";
const URL = "ws://127.0.0.1:18000/connection/websocket";

function b64url(s) {
  return Buffer.from(s).toString("base64url");
}
function mintJwt(payload, secret) {
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const data = `${head}.${body}`;
  const sig = crypto.createHmac("sha256", secret).update(data).digest("base64url");
  return `${data}.${sig}`;
}

const token = mintJwt(
  {
    sub: "u-1",
    aud: "socket",
    channels: [{ match: "chat:room:*", allow: ["sub", "pub", "history", "presence"] }],
  },
  SECRET,
);

const fail = (m) => {
  console.error("E2E FAIL:", m);
  process.exit(1);
};

const client = new SocketClient({ url: URL, getToken: async () => token });

const res = await client.connect().catch((e) => fail("connect: " + e.message));
console.log("connected as", res.client);

const sub = client.newSubscription("chat:room:1");
const received = new Promise((resolve) =>
  sub.on("publication", (p) => resolve(new TextDecoder().decode(p.data))),
);

const subRes = await sub.subscribe().catch((e) => fail("subscribe: " + e.message));
console.log("subscribed; recoverable:", subRes);

await sub.publish(new TextEncoder().encode("hello-e2e")).catch((e) => fail("publish: " + e.message));

const msg = await Promise.race([
  received,
  new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 5000)),
]).catch((e) => fail(e.message));

console.log("received:", msg);
if (msg !== "hello-e2e") fail("payload mismatch");

// presence должен содержать нас
const presence = await sub.presence().catch((e) => fail("presence: " + e.message));
console.log("presence keys:", Object.keys(presence));

client.disconnect();
console.log("E2E OK");
process.exit(0);
