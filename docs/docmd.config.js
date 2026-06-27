// Конфиг docmd (zero-config SSG). См. docs/architecture.md, раздел 10.
export default {
  srcDir: ".",
  outputDir: "../dist-docs",
  site: {
    name: "Socket",
    description: "Настраиваемый realtime-движок (WebSocket pub/sub)",
  },
  theme: { defaultMode: "dark" },
  navigation: [
    { title: "Обзор", path: "/" },
    { title: "Архитектура", path: "/architecture" },
    { title: "Прогресс", path: "/progress" },
    // TODO: автоген из proto/config → protocol.md, server-api.md, config-reference.md
  ],
};
