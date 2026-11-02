import type { Plugin, HtmlTagDescriptor, Rollup } from "vite";

import path from "node:path";

import { lookup as mimeType } from "mime-types";

const MONACO_MODULE_PATH = /(?:^|[\\/])node_modules[\\/](?:monaco-editor|@monaco-editor)[\\/]/;

// Monaco language/mode chunks load on demand when an editor needs them, so prefetching them on every page wastes bandwidth.
const isMonacoLanguageChunk = (output: Rollup.OutputChunk | Rollup.OutputAsset): boolean =>
  output.type === "chunk" &&
  output.moduleIds.length > 0 &&
  output.moduleIds.every((moduleId) => MONACO_MODULE_PATH.test(moduleId));

export function preloadTags(): Plugin {
  let basePath: string;
  return {
    name: "airbyte/preloadTags",
    apply: "build",
    configResolved(config) {
      basePath = config.base;
    },
    transformIndexHtml: {
      order: "post",
      handler(html, context) {
        const bundle = context.bundle;
        // Only put the preload tags in the index.html entry page
        if (!bundle || path.basename(context.filename) !== "index.html") {
          return html;
        }

        const tags: HtmlTagDescriptor[] = [];

        for (const [file, output] of Object.entries(bundle)) {
          if (isMonacoLanguageChunk(output)) {
            continue;
          }

          const mime = mimeType(file);
          const href = `${basePath}${file}`;
          if (mime === "application/javascript") {
            tags.push({
              tag: "link",
              attrs: {
                href,
                rel: "prefetch",
                as: "script",
              },
              injectTo: "head",
            });
          } else if (mime === "text/css") {
            tags.push({
              tag: "link",
              attrs: {
                href,
                rel: "prefetch",
                as: "style",
              },
              injectTo: "head",
            });
          }
        }

        return tags;
      },
    },
  };
}
