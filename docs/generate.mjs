// Auto-generates reference documentation from the sources of truth:
//   proto/manifold.proto → protocol.md (client protocol) + server-api.md (gRPC ServerApi)
//   config.toml        → config-reference.md
//
// Run: node docs/generate.mjs  (from the repo root)

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import protobuf from "protobufjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROTO = path.join(root, "proto/manifold.proto");
const CONFIG = path.join(root, "config.toml");
const OUT = path.join(root, "docs");

const fm = (title, desc) => `---\ntitle: ${title}\ndescription: ${desc}\n---\n\n`;
const note = "> Generated automatically from the sources of truth. Do not edit manually.\n\n";

function fieldType(f) {
  if (f.map) return `map<${f.keyType}, ${f.type}>`;
  return `${f.repeated ? "repeated " : ""}${f.type}`;
}

function messageTable(type) {
  if (!type.fieldsArray.length) return "_(empty message)_\n";
  let out = "| Field | Type | # | oneof |\n|---|---|---|---|\n";
  for (const f of type.fieldsArray) {
    const oneof = f.partOf ? `\`${f.partOf.name}\`` : "";
    out += `| \`${f.name}\` | \`${fieldType(f)}\` | ${f.id} | ${oneof} |\n`;
  }
  return out + "\n";
}

function loadProto() {
  const r = new protobuf.Root();
  r.loadSync(PROTO, { keepCase: true });
  return r.lookup("manifold.v1");
}

function isApi(name) {
  return name.endsWith("ApiRequest") || name.endsWith("ApiResponse") || name === "NodeInfo";
}

function generateProtocol(ns) {
  const messages = ns.nestedArray.filter((n) => n instanceof protobuf.Type && !isApi(n.name));
  let md = fm("Protocol", "Client WebSocket/SSE protocol (Protobuf)") + note;
  md += "Binary protobuf. Package `manifold.v1`. The client sends a `Command`, the server replies with a `Reply` " +
    "(same `id`) or an asynchronous `Push` (`id = 0`).\n\n";
  for (const t of messages) {
    md += `## ${t.name}\n\n${messageTable(t)}`;
  }
  return md;
}

function generateServerApi(ns) {
  const svc = ns.nestedArray.find((n) => n instanceof protobuf.Service);
  const apiMsgs = ns.nestedArray.filter((n) => n instanceof protobuf.Type && isApi(n.name));

  let md = fm("Server API", "Server-side API (HTTP + gRPC) for publishing from the backend") + note;
  md += "Trusted server-to-server side. The gRPC service is below; HTTP/JSON exposes the same methods at " +
    "`POST /api/<method>`. Auth — `Authorization: apikey <key>`.\n\n";

  if (svc) {
    md += `## Service \`${svc.name}\`\n\n`;
    md += "| Method | Request | Response | Stream |\n|---|---|---|---|\n";
    for (const m of svc.methodsArray) {
      const stream = `${m.requestStream ? "↑" : ""}${m.responseStream ? "↓" : ""}` || "—";
      md += `| \`${m.name}\` | \`${m.requestType}\` | \`${m.responseType}\` | ${stream} |\n`;
    }
    md += "\n";
  }
  md += "## Messages\n\n";
  for (const t of apiMsgs) {
    md += `### ${t.name}\n\n${messageTable(t)}`;
  }
  return md;
}

function generateConfig() {
  const lines = fs.readFileSync(CONFIG, "utf8").split("\n");
  let md = fm("Config reference", "All config.toml keys") + note;
  md += "Source — `config.toml`. Secrets are set via `${ENV_VAR}`.\n\n";
  let openTable = false;
  const closeTable = () => {
    if (openTable) md += "\n";
    openTable = false;
  };

  for (const raw of lines) {
    const line = raw.trim();
    if (!line || line.startsWith("# ─") || line.startsWith("#  ")) continue;
    // section
    const sec = line.match(/^\[+([^\]]+)\]+/);
    if (sec) {
      closeTable();
      md += `## \`[${sec[1]}]\`\n\n`;
      continue;
    }
    // key = value  # comment
    const kv = line.match(/^([A-Za-z0-9_]+)\s*=\s*(.+?)(?:\s+#\s*(.*))?$/);
    if (kv) {
      if (!openTable) {
        md += "| Key | Example | Description |\n|---|---|---|\n";
        openTable = true;
      }
      const [, key, value, comment] = kv;
      const val = value.replace(/\|/g, "\\|").slice(0, 60);
      md += `| \`${key}\` | \`${val}\` | ${comment ? comment.replace(/\|/g, "\\|") : ""} |\n`;
    }
  }
  closeTable();
  return md;
}

const ns = loadProto();
fs.writeFileSync(path.join(OUT, "protocol.md"), generateProtocol(ns));
fs.writeFileSync(path.join(OUT, "server-api.md"), generateServerApi(ns));
fs.writeFileSync(path.join(OUT, "config-reference.md"), generateConfig());
console.log("✓ docs: protocol.md, server-api.md, config-reference.md");
