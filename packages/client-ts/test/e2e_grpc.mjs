// E2E gRPC Server API: gRPC Publish → WS subscriber receives the message.
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import { SocketClient } from "../dist/index.js";

const SECRET = "dev-secret";
const WS_URL = "ws://127.0.0.1:18000/connection/websocket";
const GRPC_ADDR = "127.0.0.1:18002";
const API_KEY = "test-key";

const here = path.dirname(fileURLToPath(import.meta.url));
const PROTO = path.resolve(here, "../../../proto/socket.proto");

const fail = (m) => {
  console.error("E2E GRPC FAIL:", m);
  process.exit(1);
};

const b64url = (s) => Buffer.from(s).toString("base64url");
function mintJwt(payload, secret) {
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const data = `${head}.${body}`;
  return `${data}.${crypto.createHmac("sha256", secret).update(data).digest("base64url")}`;
}
const token = mintJwt(
  { sub: "u-g", aud: "socket", channels: [{ match: "chat:room:*", allow: ["sub"] }] },
  SECRET,
);

// gRPC client
const def = protoLoader.loadSync(PROTO, { keepCase: true, longs: String, defaults: true });
const pkg = grpc.loadPackageDefinition(def);
const Svc = pkg.socket.v1.ServerApi;
const grpcClient = new Svc(GRPC_ADDR, grpc.credentials.createInsecure());

// WS subscriber
const client = new SocketClient({ url: WS_URL, getToken: async () => token });
await client.connect().catch((e) => fail("ws connect: " + e.message));
const sub = client.newSubscription("chat:room:1");
const received = new Promise((res) => sub.on("publication", (p) => res(new TextDecoder().decode(p.data))));
await sub.subscribe().catch((e) => fail("subscribe: " + e.message));
console.log("WS subscriber up");

// gRPC Publish
const meta = new grpc.Metadata();
meta.add("authorization", `apikey ${API_KEY}`);
const resp = await new Promise((resolve, reject) =>
  grpcClient.Publish(
    { channel: "chat:room:1", data: Buffer.from("grpc-pub") },
    meta,
    (err, r) => (err ? reject(err) : resolve(r)),
  ),
).catch((e) => fail("grpc publish: " + e.message));
console.log("gRPC publish resp: offset", resp.offset, "epoch", resp.epoch);

const msg = await Promise.race([received, new Promise((_, r) => setTimeout(() => r(new Error("timeout")), 5000))]).catch(
  (e) => fail(e.message),
);
console.log("WS received:", msg);
if (msg !== "grpc-pub") fail("payload mismatch");

client.disconnect();
grpcClient.close();
console.log("E2E GRPC OK");
process.exit(0);
