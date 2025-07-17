import type { Plugin } from "vite";

import fs from "fs";
import path from "path";

import chalk from "chalk";
import { loadEnv } from "vite";

// config.root is not set when environmentVariables runs, so we need a different reference to the root path
const ROOT_PATH = path.join(__dirname, `../../`);

/**
 * A Vite plugin that determines all the environment variables for the build process
 */
export function environmentVariables(): Plugin {
  return {
    name: "airbyte/environment-variables",
    config: (_config, { command, mode }) => {
      // Load any cloud-specific .env files
      let additionalVariables = {};
      const envPath = process.env.WEBAPP_ENV_PATH;

      if (envPath) {
        if (!fs.existsSync(envPath)) {
          throw new Error(`Could not find .env file at ${envPath}. Are you sure this file exists?`);
        }
        console.log(`Using custom env file at ${chalk.green(envPath)}\n`);
        additionalVariables = { ...loadEnv(mode, path.dirname(envPath), ["REACT_APP_"]) };
      }

      const cloudEnv = process.env.WEBAPP_BUILD_CLOUD_ENV;

      if (cloudEnv && !envPath) {
        console.log(`☁️ Getting env file for cloud environment ${chalk.green(cloudEnv)}\n`);
        const envDirPath = path.join(ROOT_PATH, `../../cloud/cloud-webapp/envs/`, cloudEnv);

        // loadEnv will not throw if you give it a non-existent path, so we explicitly check here
        if (!fs.existsSync(path.join(envDirPath, `.env`))) {
          throw new Error(
            `Could not find .env file for environment ${cloudEnv}. Are you sure this file exists? WEBAPP_BUILD_CLOUD_ENV is only intended for internal Airbyte users.`
          );
        }

        additionalVariables = { REACT_APP_CLOUD: "true", ...loadEnv(mode, envDirPath, ["REACT_APP_"]) };
      }

      // Environment variables that should be available in the frontend
      const frontendEnvVariables = loadEnv(mode, ROOT_PATH, ["REACT_APP_"]);

      // Mirrors the backend version which is set during deployment in .github/actions/deploy/action.yaml
      const version = JSON.stringify(
        `${process.env.AIRBYTE_VERSION || "dev"}${process.env.WEBAPP_BUILD_CLOUD_ENV ? "-cloud" : ""}`
      );

      // Create an object of defines that will shim all required process.env variables.
      const processEnv = {
        "process.env.REACT_APP_VERSION": version,
        "process.env.NODE_ENV": JSON.stringify(mode),
        ...Object.fromEntries([
          // Only use frontendEnvVariables if envPath is undefined
          ...Object.entries({ ...frontendEnvVariables, ...additionalVariables }).map(([key, value]) => [
            `process.env.${key}`,
            JSON.stringify(value),
          ]),
        ]),
      };

      if (command === "build") {
        console.log(`Replacing the following process.env values in the frontend code:\n`, processEnv);
      }

      return {
        define: {
          ...processEnv,
          // This lets us set the version in a meta tag in index.html
          "import.meta.env.VERSION": version,
        },
      };
    },
  };
}
