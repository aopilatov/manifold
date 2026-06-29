// E2E multinode: a subscriber on node 2 receives a publication made via node 1 (shared Redis).
import crypto from "node:crypto";
import { SocketClient } from "../dist/index.js";

const SECRET = "dev-secret";
const NODE1 = "ws://127.0.0.1:18000/connection/websocket";
const NODE2 = "ws://127.0.0.1:19000/connection/websocket";

const b64url = (s) => Buffer.from(s).toString("base64url");
function mintJwt(payload, secret) {
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const data = `${head}.${body}`;
  return `${data}.${crypto.createHmac("sha256", secret).update(data).digest("base64url")}`;
}
const token = mintJwt(
  { sub: "u-x", aud: "socket", channels: [{ match: "chat:room:*", allow: ["sub", "pub", "presence"] }] },
  SECRET,
);
const fail = (m) => {
  console.error("E2E FAIL:", m);
  process.exit(1);
};

// Subscriber on NODE 2
const clientB = new SocketClient({ url: NODE2, getToken: async () => token });
await clientB.connect().catch((e) => fail("B connect: " + e.message));
const subB = clientB.newSubscription("chat:room:1");
const received = new Promise((res) =>
  subB.on("publication", (p) => res(new TextDecoder().decode(p.data))),
);
await subB.subscribe().catch((e) => fail("B subscribe: " + e.message));
console.log("subscriber up on node-2");

// Publisher on NODE 1 (no subscription)
const clientA = new SocketClient({ url: NODE1, getToken: async () => token });
await clientA.connect().catch((e) => fail("A connect: " + e.message));
const subA = clientA.newSubscription("chat:room:1");
await subA.publish(new TextEncoder().encode("xnode-hello")).catch((e) => fail("A publish: " + e.message));
console.log("published via node-1");

const msg = await Promise.race([
  received,
  new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 5000)),
]).catch((e) => fail(e.message));

console.log("node-2 received:", msg);
if (msg !== "xnode-hello") fail("payload mismatch");

clientA.disconnect();
clientB.disconnect();
console.log("E2E MULTINODE OK");
process.exit(0);
