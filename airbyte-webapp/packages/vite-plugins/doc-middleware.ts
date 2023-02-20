import type { Connect, Plugin, ViteDevServer } from "vite";

import fs from "fs";
import path from "path";

import chalk from "chalk";
import express from "express";

const AIRBYTE_REPO_PATH = `${path.resolve(__dirname, "../../../../airbyte")}`;
const INTEGRATIONS_DOCS_DIR = `${AIRBYTE_REPO_PATH}/docs/integrations`;

const localDocMiddleware = (): Plugin => {
  return {
    name: "airbyte/doc-middleware-local",
    configureServer(server: ViteDevServer) {
      // Serve the docs used in the sidebar. During building Gradle will copy those into the docker image
      // Relavant gradle task :airbyte-webapp:copyDocs
      server.middlewares.use("/docs/integrations", express.static(INTEGRATIONS_DOCS_DIR) as Connect.NextHandleFunction);
      // workaround for adblockers to serve google ads docs in development
      server.middlewares.use(
        "/docs/integrations/sources/gglad.md",
        express.static(`${INTEGRATIONS_DOCS_DIR}/sources/google-ads.md`) as Connect.NextHandleFunction
      );
      // Server assets that can be used during. Related gradle task: :airbyte-webapp:copyDocAssets
      server.middlewares.use(
        "/docs/.gitbook",
        express.static(`${AIRBYTE_REPO_PATH}/docs/.gitbook`) as Connect.NextHandleFunction
      );
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
                rewrite: (path) => (path.endsWith("gglad.md") ? path.replace(/\/gglad\.md$/, "/google-ads.md") : path),
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

export function docMiddleware(): Plugin {
  const isAirbyteCheckedOut = fs.existsSync(INTEGRATIONS_DOCS_DIR) && fs.statSync(INTEGRATIONS_DOCS_DIR).isDirectory();

  if (isAirbyteCheckedOut) {
    console.log(
      `📃 Connector docs are served ${chalk.bold.cyan("locally")} from ${chalk.green(INTEGRATIONS_DOCS_DIR)}.\n`
    );
    return localDocMiddleware();
  }

  console.log(`📃 Connector docs are served ${chalk.bold.magenta("remotely")} from GitHub.`);
  console.log(
    `📃 To work with local docs checkout ${chalk.bold.gray(
      "https://github.com/airbytehq/airbyte"
    )} in parallel to your ${path.basename(path.resolve(__dirname, "../../.."))} folder.\n`
  );
  return remoteDocMiddleware();
}
