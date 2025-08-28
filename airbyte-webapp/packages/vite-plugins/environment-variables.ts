import type { Plugin } from "vite";

import path from "path";

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
          ...Object.entries(frontendEnvVariables).map(([key, value]) => [`process.env.${key}`, JSON.stringify(value)]),
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
