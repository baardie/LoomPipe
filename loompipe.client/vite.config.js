import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    https: false,
    proxy: {
      '/api': {
        target: 'http://localhost:5259',
        changeOrigin: true,
        secure: false,
      }
    }
  }
})
