// docmd config (zero-config SSG). See docs/architecture.md, section 10.
export default {
  siteTitle: "Socket",
  title: "Socket",
  description: "Configurable realtime engine (WebSocket pub/sub)",
  srcDir: ".",
  outputDir: "../dist-docs",
  theme: {
    name: "default",
    defaultMode: "dark",
  },
  navigation: [
    { title: "Overview", path: "/" },
    { title: "Architecture", path: "/architecture" },
    { title: "Progress", path: "/progress" },
    { title: "Protocol", path: "/protocol" },
    { title: "Server API", path: "/server-api" },
    { title: "Config reference", path: "/config-reference" },
  ],
};
