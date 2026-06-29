import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // dev: proxy to the engine's admin port (8003 by default from config.toml)
      "/admin": "http://127.0.0.1:8003",
    },
  },
  build: { outDir: "dist" },
});
