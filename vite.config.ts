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
    // Strip the build hash from shared chunks so bundle diffs do not churn
    // on every rebuild. Content-addressed naming is the rolldown default;
    // we trade that for diff stability since this package's chunks have
    // unique names that do not collide.
    outputOptions: {
      chunkFileNames: "[name].js",
    },
  },
})
