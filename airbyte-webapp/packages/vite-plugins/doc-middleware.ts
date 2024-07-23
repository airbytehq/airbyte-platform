import type { Connect, Plugin, ViteDevServer } from "vite";

import fs from "fs";
import path from "path";

import chalk from "chalk";
import express from "express";

const localDocMiddleware = (docsPath: string): Plugin => {
  return {
    name: "airbyte/doc-middleware-local",
    configureServer(server: ViteDevServer) {
      // Serve the docs used in the sidebar. In dev mode, docs are served from either the local airbyte repository,
      // or github if that repository is not found.
      server.middlewares.use("/docs/integrations", express.static(docsPath) as Connect.NextHandleFunction);
      // Don't fallback to default handling (serve index.html) for not found files, but make sure they 404 out properly
      // so that a clear error message is displayed in the documentation panel if the file is not found.
      server.middlewares.use("/docs/integrations", (req, res) => {
        res.statusCode = 404;
        res.end(`404 - ${docsPath}${req.url} not found`);
      });
      // Server assets that can be used during. Related gradle task: :oss:airbyte-webapp:copyDocAssets
      server.middlewares.use("/docs/.gitbook", express.static(`${docsPath}/../.gitbook`) as Connect.NextHandleFunction);
    },
  };
};

const remoteDocMiddleware = (): Plugin => {
  return {
    name: "airbyte/doc-middleware-remote",
    config(config, { command }) {
      if (command !== "build") {
        return {
          server: {
            // Proxy requests to connector docs and assets directly to GitHub to load them from airbytehq/airbyte repository.
            proxy: {
              "/docs/integrations": {
                changeOrigin: true,
                target: "https://raw.githubusercontent.com/airbytehq/airbyte/master",
              },
              "/docs/.gitbook": {
                changeOrigin: true,
                target: "https://raw.githubusercontent.com/airbytehq/airbyte/master",
              },
            },
          },
        };
      }
    },
  };
};

const getDocsIntegrationPath = (searchPath: string): string => {
  return path.resolve(__dirname, searchPath, "airbyte/docs/integrations");
};

/**
 * Tries to find the airbytehq/airbyte repository checked out in parallel to the current repository
 * for serving docs locally. Returns {@code null} if it can't be found.
 */
const getLocalDocsPath = (): string | null => {
  // If running inside airbyte-platform this file is nested 4 folders deep into the repository
  const usingAirbytePlatformPath = getDocsIntegrationPath("../../../../");
  if (fs.existsSync(usingAirbytePlatformPath) && fs.statSync(usingAirbytePlatformPath).isDirectory()) {
    return usingAirbytePlatformPath;
  }

  // If running inside airbyte-platform-internal this file is nested 5 folders deep into the repository
  const usingAirbytePlatformInternalPath = getDocsIntegrationPath("../../../../../");
  if (fs.existsSync(usingAirbytePlatformInternalPath) && fs.statSync(usingAirbytePlatformInternalPath).isDirectory()) {
    return usingAirbytePlatformInternalPath;
  }

  return null;
};

export function docMiddleware(): Plugin {
  const localPath = getLocalDocsPath();

  if (localPath) {
    console.log(`📃 Connector docs are served ${chalk.bold.cyan("locally")} from ${chalk.green(localPath)}.\n`);
    return localDocMiddleware(localPath);
  }

  console.log(`📃 Connector docs are served ${chalk.bold.magenta("remotely")} from GitHub.`);
  console.log(
    `📃 To work with local docs checkout ${chalk.bold.gray(
      "https://github.com/airbytehq/airbyte"
    )} next to your ${chalk.bold.gray("airbyte-platform")} or ${chalk.bold.gray("airbyte-platform-internal")} folder.\n`
  );
  return remoteDocMiddleware();
}
