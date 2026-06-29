// Конфиг docmd (zero-config SSG). См. docs/architecture.md, раздел 10.
export default {
  siteTitle: "Socket",
  title: "Socket",
  description: "Настраиваемый realtime-движок (WebSocket pub/sub)",
  srcDir: ".",
  outputDir: "../dist-docs",
  theme: {
    name: "default",
    defaultMode: "dark",
  },
  navigation: [
    { title: "Обзор", path: "/" },
    { title: "Архитектура", path: "/architecture" },
    { title: "Прогресс", path: "/progress" },
    { title: "Протокол", path: "/protocol" },
    { title: "Server API", path: "/server-api" },
    { title: "Справочник конфига", path: "/config-reference" },
  ],
};
