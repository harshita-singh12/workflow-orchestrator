import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev-server proxy so the dashboard can call relative /api paths in both dev (Vite serving
// on :5173) and prod (nginx serving the built assets + /api reverse-proxied to the server
// container, see docker-compose.yml and frontend/nginx.conf) without an env-specific base URL.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": { target: "http://localhost:8080", changeOrigin: true },
      "/healthz": { target: "http://localhost:8080", changeOrigin: true },
    },
  },
});
