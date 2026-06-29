# manifold-proto-gen

Generated [protobuf-es](https://github.com/bufbuild/protobuf-es) types for the
[Manifold](https://hub.docker.com/r/saxikopilatov/manifold) realtime protocol (`manifold.v1`).

This package is a dependency of **[manifold-client](https://www.npmjs.com/package/manifold-client)** —
you normally don't install it directly. Use it only if you need the raw message schemas/types
(`Command`, `Reply`, `Publication`, the `ServerApi` gRPC types, etc.) to build a custom client or
tooling.

```bash
npm install manifold-proto-gen @bufbuild/protobuf
```

```ts
import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import { CommandSchema, ReplySchema, type Command } from "manifold-proto-gen";

const bytes = toBinary(CommandSchema, create(CommandSchema, {
  id: 1,
  method: { case: "ping", value: {} },
}));

const reply = fromBinary(ReplySchema, incomingBytes);
```

Regenerated from `proto/manifold.proto` via `buf` + `protoc-gen-es`.

## License

MIT
