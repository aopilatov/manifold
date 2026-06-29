import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // dev: проксируем на admin-порт движка (8003 по умолчанию из config.toml)
      "/admin": "http://127.0.0.1:8003",
    },
  },
  build: { outDir: "dist" },
});
