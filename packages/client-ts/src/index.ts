// Public API of the client SDK.
//
//   const client = new ManifoldClient({ url, getToken });
//   await client.connect();
//   const sub = client.newSubscription("chat:room:42");
//   sub.on("publication", (p) => console.log(p.data));
//   await sub.subscribe();
//   await sub.publish(new TextEncoder().encode("hi"));

export { ManifoldClient, Subscription, type ManifoldOptions } from "./client.js";
export { jitteredDelay, type BackoffOptions } from "./backoff.js";
export { encodeCommand, decodeReply } from "./codec.js";
