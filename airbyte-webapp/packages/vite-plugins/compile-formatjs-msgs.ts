import type { Plugin } from "vite";

import { parse } from "@formatjs/icu-messageformat-parser";

export function compileFormatJsMessages(): Plugin {
  return {
    name: "airbyte/compile-formatjs-messages",
    enforce: "pre",
    transform(code: string, id: string) {
      // Transform all JSON files under /locales/
      if (/\/locales\/.+\.json$/.test(id)) {
        const messages = JSON.parse(code);
        // Parse every message using the formatjs utils into an AST
        const compiledMsgs = Object.fromEntries(
          Object.entries(messages).map(([key, i18nmsg]) => [key, parse(i18nmsg as string)])
        );
        return JSON.stringify(compiledMsgs);
      }
    },
  };
}
