import { fileURLToPath } from "node:url"
import { defineConfig } from "vite-plus"

export default defineConfig({
  resolve: { alias: { "@": fileURLToPath(new URL("./lib", import.meta.url)) } },
  fmt: { semi: false },
  lint: {
    ignorePatterns: ["node_modules/**"],
  },
  pack: {
    entry: {
      index: "lib/index.ts",
      slack: "lib/slack.ts",
      discord: "lib/discord.ts",
      github: "lib/github.ts",
    },
    format: "esm",
    dts: true,
    outExtensions: () => ({ js: ".js" }),
  },
})
