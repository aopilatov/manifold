// E2E Server API: HTTP publish on node-1 → subscriber on node-2; then HTTP disconnect across the cluster.
import crypto from "node:crypto";
import { SocketClient } from "../dist/index.js";

const SECRET = "dev-secret";
const NODE2_WS = "ws://127.0.0.1:19000/connection/websocket";
const NODE1_HTTP = "http://127.0.0.1:18001/api";
const API_KEY = "test-key";

const b64url = (s) => Buffer.from(s).toString("base64url");
function mintJwt(payload, secret) {
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const data = `${head}.${body}`;
  return `${data}.${crypto.createHmac("sha256", secret).update(data).digest("base64url")}`;
}
const token = mintJwt(
  { sub: "u-x", aud: "socket", channels: [{ match: "chat:room:*", allow: ["sub", "presence"] }] },
  SECRET,
);
const fail = (m) => {
  console.error("E2E SERVERAPI FAIL:", m);
  process.exit(1);
};
const apiPost = (path, body) =>
  fetch(`${NODE1_HTTP}/${path}`, {
    method: "POST",
    headers: { authorization: `apikey ${API_KEY}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });

// Subscriber on NODE 2
const clientB = new SocketClient({ url: NODE2_WS, getToken: async () => token });
clientB.disconnect = clientB.disconnect.bind(clientB);
await clientB.connect().catch((e) => fail("B connect: " + e.message));
const subB = clientB.newSubscription("chat:room:1");
const gotPub = new Promise((res) => subB.on("publication", (p) => res(new TextDecoder().decode(p.data))));
const gotDisc = new Promise((res) => clientB.on("disconnected", () => res(true)));
await subB.subscribe().catch((e) => fail("B subscribe: " + e.message));
console.log("subscriber up on node-2");

// 1) HTTP publish via NODE 1
const data = Buffer.from("server-pub").toString("base64");
const pr = await apiPost("publish", { channel: "chat:room:1", data });
if (pr.status !== 200) fail("publish status " + pr.status);
console.log("HTTP publish ok:", await pr.json());

const msg = await Promise.race([gotPub, new Promise((_, r) => setTimeout(() => r(new Error("pub timeout")), 5000))]).catch(
  (e) => fail(e.message),
);
console.log("node-2 received:", msg);
if (msg !== "server-pub") fail("payload mismatch");

// 2) presence over HTTP should see the subscriber (shared Redis)
const presR = await apiPost("presence", { channel: "chat:room:1" });
const pres = await presR.json();
console.log("presence:", Object.keys(pres.presence ?? {}).length, "client(s)");

// 3) HTTP disconnect by user → cluster control → node-2 drops clientB
clientB.disconnect = () => {}; // don't let the SDK silently reconnect — watch for the event
const dr = await apiPost("disconnect", { user: "u-x", reason: "by-admin" });
if (dr.status !== 202) fail("disconnect status " + dr.status);
console.log("HTTP disconnect sent");

const disc = await Promise.race([gotDisc, new Promise((_, r) => setTimeout(() => r(new Error("disc timeout")), 5000))]).catch(
  (e) => fail(e.message),
);
console.log("clientB disconnected:", disc);

console.log("E2E SERVERAPI OK");
process.exit(0);
