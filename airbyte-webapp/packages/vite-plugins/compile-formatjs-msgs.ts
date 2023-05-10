import type { Plugin } from "vite";

import { parse } from "@formatjs/icu-messageformat-parser";

export function compileFormatJsMessages(): Plugin {
  return {
    name: "airbyte/compile-formatjs-messages",
    transform(code: string, id: string) {
      // Transform all JSON files under /locales/
      if (/\/locales\/.+\.json$/.test(id)) {
        if (!code.startsWith("export default {") || !code.endsWith("};\n")) {
          throw new Error(`Tried to compile JSON locale file, but got an unexpected format. (${id})`);
        }
        // Remove JS export, to extract raw JSON from that file
        const rawJson = code.replace(/^export default {/, "{").replace(/\};\n$/, "}");
        const messages = JSON.parse(rawJson);
        // Parse every message using the formatjs utils into an AST
        const compiledMsgs = Object.fromEntries(
          Object.entries(messages).map(([key, i18nmsg]) => [key, parse(i18nmsg as string)])
        );
        return `export default ${JSON.stringify(compiledMsgs)};`;
      }
    },
  };
}
