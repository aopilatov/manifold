// Кодек протокола: Command ⇄ bytes, bytes → Reply (protobuf-es).
import { toBinary, fromBinary } from "@bufbuild/protobuf";
import {
  CommandSchema,
  ReplySchema,
  type Command,
  type Reply,
} from "@socket/proto-gen";

export function encodeCommand(cmd: Command): Uint8Array {
  return toBinary(CommandSchema, cmd);
}

export function decodeReply(bytes: Uint8Array): Reply {
  return fromBinary(ReplySchema, bytes);
}
