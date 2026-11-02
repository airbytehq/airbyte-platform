import type { Plugin, HtmlTagDescriptor, Rollup } from "vite";

import path from "node:path";

import { lookup as mimeType } from "mime-types";

const MONACO_MODULE_PATH = /(?:^|[\\/])node_modules[\\/](?:monaco-editor|@monaco-editor)[\\/]/;

// Monaco language/mode chunks load on demand when an editor needs them, so prefetching them on every page wastes bandwidth.
const isMonacoLanguageChunk = (output: Rollup.OutputChunk | Rollup.OutputAsset): boolean =>
  output.type === "chunk" &&
  output.moduleIds.length > 0 &&
  output.moduleIds.every((moduleId) => MONACO_MODULE_PATH.test(moduleId));

type RouteChunkMap = Record<string, { js: string[]; css: string[] }>;

const buildRouteChunkMap = (
  bundle: Rollup.OutputBundle,
  sourceRoot: string,
  basePath: string,
  entryFiles: Set<string>
): RouteChunkMap => {
  const routeChunks: RouteChunkMap = {};

  for (const output of Object.values(bundle)) {
    if (output.type !== "chunk" || !output.isDynamicEntry) {
      continue;
    }

    const matchingModuleIds = output.moduleIds.filter((moduleId) => {
      const sourceModuleId = moduleId.split("?")[0];
      const modulePath = path.relative(sourceRoot, sourceModuleId);
      const isSourceModule = !modulePath.startsWith("..") && !path.isAbsolute(modulePath);

      return isSourceModule && path.basename(sourceModuleId, path.extname(sourceModuleId)) === output.name;
    });
    const moduleId = output.facadeModuleId ?? (matchingModuleIds.length === 1 ? matchingModuleIds[0] : undefined);
    const relativePath = moduleId ? path.relative(sourceRoot, moduleId) : undefined;
    if (!relativePath || relativePath.startsWith("..") || path.isAbsolute(relativePath)) {
      continue;
    }

    const moduleKey = relativePath
      .split(path.sep)
      .join("/")
      .replace(/\.[^/.]+$/, "")
      .replace(/\/index$/, "");
    const jsFiles = new Set<string>();
    const cssFiles = new Set<string>();
    const visited = new Set<string>();

    const visitDynamicChunk = (chunkFile: string) => {
      if (visited.has(chunkFile) || entryFiles.has(chunkFile)) {
        return;
      }

      visited.add(chunkFile);
      const chunk = bundle[chunkFile];
      if (!chunk || chunk.type !== "chunk") {
        return;
      }

      jsFiles.add(`${basePath}${chunk.fileName}`);
      chunk.viteMetadata?.importedCss.forEach((cssFile) => {
        if (!entryFiles.has(cssFile)) {
          cssFiles.add(`${basePath}${cssFile}`);
        }
      });
      chunk.imports.forEach(visitDynamicChunk);
    };

    visitDynamicChunk(output.fileName);
    routeChunks[moduleKey] = { js: [...jsFiles], css: [...cssFiles] };
  }

  return routeChunks;
};

export function preloadTags(): Plugin {
  let basePath: string;
  let sourceRoot: string;
  return {
    name: "airbyte/preloadTags",
    apply: "build",
    configResolved(config) {
      basePath = config.base;
      sourceRoot = path.join(config.root, "src");
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
        const entryFiles = new Set<string>();
        const visitEntryChunk = (fileName: string) => {
          if (entryFiles.has(fileName)) {
            return;
          }

          entryFiles.add(fileName);
          const output = bundle[fileName];
          if (!output || output.type !== "chunk") {
            return;
          }

          output.viteMetadata?.importedCss.forEach((cssFile) => entryFiles.add(cssFile));
          output.imports.forEach(visitEntryChunk);
        };

        Object.values(bundle).forEach((output) => {
          if (output.type === "chunk" && output.isEntry) {
            visitEntryChunk(output.fileName);
          }
        });

        const routeChunks = buildRouteChunkMap(bundle, sourceRoot, basePath, entryFiles);

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

        if (Object.keys(routeChunks).length > 0) {
          tags.push({
            tag: "script",
            attrs: {
              type: "application/json",
              id: "airbyte-route-chunks",
            },
            children: JSON.stringify(routeChunks).replace(/</g, "\\u003c"),
            injectTo: "head",
          });
        }

        return tags;
      },
    },
  };
}
