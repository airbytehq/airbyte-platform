import type { Plugin, HtmlTagDescriptor } from "vite";

import { lookup as mimeType } from "mime-types";

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
        if (!bundle) {
          return html;
        }

        const tags: HtmlTagDescriptor[] = [];

        for (const file of Object.keys(bundle)) {
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
