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
    config: (_config, { mode }) => {
      console.log(`🌍 Determining environment variables for env ${chalk.cyan(mode)}\n`);

      // Load any cloud-specific .env files
      let cloudEnvVariables = {};
      const cloudEnv = process.env.WEBAPP_BUILD_CLOUD_ENV;
      if (cloudEnv) {
        console.log(`  - Getting env file for cloud environment ${chalk.green(cloudEnv)}\n`);
        const envDirPath = path.join(ROOT_PATH, `../../cloud-webapp/envs/`, cloudEnv);

        // loadEnv will not throw if you give it a non-existent path, so we explicitly check here
        if (!fs.existsSync(path.join(envDirPath, `.env`))) {
          throw new Error(
            `Could not find .env file for environment ${cloudEnv}. Are you sure this file exists? WEBAPP_BUILD_CLOUD_ENV is only intended for internal Airbyte users.`
          );
        }

        cloudEnvVariables = loadEnv(mode, envDirPath, ["REACT_APP_"]);
      }

      // Load variables from all .env files
      process.env = {
        ...process.env,
        ...loadEnv(mode, ROOT_PATH, ""),
      };

      // Environment variables that should be available in the frontend
      const frontendEnvVariables = loadEnv(mode, ROOT_PATH, ["REACT_APP_"]);

      // Create an object of defines that will shim all required process.env variables.
      const processEnv = {
        "process.env.REACT_APP_VERSION": JSON.stringify(
          `${process.env.VERSION}${process.env.WEBAPP_BUILD_CLOUD_ENV ? "-cloud" : ""}`
        ),
        "process.env.NODE_ENV": JSON.stringify(mode),
        ...Object.fromEntries([
          // Any cloud .env files should overwrite OSS .env files
          ...Object.entries({ ...frontendEnvVariables, ...cloudEnvVariables }).map(([key, value]) => [
            `process.env.${key}`,
            JSON.stringify(value),
          ]),
        ]),
      };

      return {
        define: {
          ...processEnv,
        },
      };
    },
  };
}
