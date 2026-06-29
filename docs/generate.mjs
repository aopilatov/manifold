// Автоген справочной документации из источников истины:
//   proto/socket.proto → protocol.md (клиентский протокол) + server-api.md (gRPC ServerApi)
//   config.toml        → config-reference.md
//
// Запуск: node docs/generate.mjs  (из корня репо)

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import protobuf from "protobufjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROTO = path.join(root, "proto/socket.proto");
const CONFIG = path.join(root, "config.toml");
const OUT = path.join(root, "docs");

const fm = (title, desc) => `---\ntitle: ${title}\ndescription: ${desc}\n---\n\n`;
const note = "> Сгенерировано автоматически из источников истины. Не редактировать вручную.\n\n";

function fieldType(f) {
  if (f.map) return `map<${f.keyType}, ${f.type}>`;
  return `${f.repeated ? "repeated " : ""}${f.type}`;
}

function messageTable(type) {
  if (!type.fieldsArray.length) return "_(пустое сообщение)_\n";
  let out = "| Поле | Тип | № | oneof |\n|---|---|---|---|\n";
  for (const f of type.fieldsArray) {
    const oneof = f.partOf ? `\`${f.partOf.name}\`` : "";
    out += `| \`${f.name}\` | \`${fieldType(f)}\` | ${f.id} | ${oneof} |\n`;
  }
  return out + "\n";
}

function loadProto() {
  const r = new protobuf.Root();
  r.loadSync(PROTO, { keepCase: true });
  return r.lookup("socket.v1");
}

function isApi(name) {
  return name.endsWith("ApiRequest") || name.endsWith("ApiResponse") || name === "NodeInfo";
}

function generateProtocol(ns) {
  const messages = ns.nestedArray.filter((n) => n instanceof protobuf.Type && !isApi(n.name));
  let md = fm("Протокол", "Клиентский WebSocket/SSE-протокол (Protobuf)") + note;
  md += "Бинарный protobuf. Пакет `socket.v1`. Клиент шлёт `Command`, сервер отвечает `Reply` " +
    "(тот же `id`) либо асинхронным `Push` (`id = 0`).\n\n";
  for (const t of messages) {
    md += `## ${t.name}\n\n${messageTable(t)}`;
  }
  return md;
}

function generateServerApi(ns) {
  const svc = ns.nestedArray.find((n) => n instanceof protobuf.Service);
  const apiMsgs = ns.nestedArray.filter((n) => n instanceof protobuf.Type && isApi(n.name));

  let md = fm("Server API", "Серверный API (HTTP + gRPC) для публикации из бэкенда") + note;
  md += "Доверенная server-to-server сторона. gRPC-сервис ниже; HTTP/JSON — те же методы на " +
    "`POST /api/<method>`. Auth — `Authorization: apikey <key>`.\n\n";

  if (svc) {
    md += `## Сервис \`${svc.name}\`\n\n`;
    md += "| Метод | Запрос | Ответ | Стрим |\n|---|---|---|---|\n";
    for (const m of svc.methodsArray) {
      const stream = `${m.requestStream ? "↑" : ""}${m.responseStream ? "↓" : ""}` || "—";
      md += `| \`${m.name}\` | \`${m.requestType}\` | \`${m.responseType}\` | ${stream} |\n`;
    }
    md += "\n";
  }
  md += "## Сообщения\n\n";
  for (const t of apiMsgs) {
    md += `### ${t.name}\n\n${messageTable(t)}`;
  }
  return md;
}

function generateConfig() {
  const lines = fs.readFileSync(CONFIG, "utf8").split("\n");
  let md = fm("Справочник конфига", "Все ключи config.toml") + note;
  md += "Источник — `config.toml`. Секреты задаются через `${ENV_VAR}`.\n\n";
  let openTable = false;
  const closeTable = () => {
    if (openTable) md += "\n";
    openTable = false;
  };

  for (const raw of lines) {
    const line = raw.trim();
    if (!line || line.startsWith("# ─") || line.startsWith("#  ")) continue;
    // секция
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
        md += "| Ключ | Пример | Описание |\n|---|---|---|\n";
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
