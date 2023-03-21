import type { Plugin } from "vite";

import fs from "fs";
import path from "path";

import chalk from "chalk";

const EXPERIMENTS_FILE = path.resolve(__dirname, "../../.experiments.json");

export function experimentOverwrites(): Plugin {
  return {
    name: "airbyte/experiment-service",
    config(_config, { command }) {
      if (command === "serve" && fs.existsSync(EXPERIMENTS_FILE)) {
        const overwriteJson = fs.readFileSync(EXPERIMENTS_FILE, "utf8");
        try {
          const overwrites = JSON.parse(overwriteJson);

          if (Object.keys(overwrites).length) {
            console.log(chalk.bold(`🧪 Overwriting experiments via ${chalk.green(".experiments.json")}`));
            Object.entries(overwrites).forEach(([key, value]) => {
              console.log(`   ➜ ${chalk.cyan(key)}: ${JSON.stringify(value)}`);
            });
            return {
              define: {
                "process.env.REACT_APP_EXPERIMENT_OVERWRITES": overwriteJson,
              },
            };
          }
        } catch (e) {
          if (overwriteJson) {
            // ignore error if the file is just empty
            console.log(
              chalk.red(`🧪 Could not parse ${chalk.bold(".experiments.json")} file: ${chalk.bold(e.message)}`)
            );
          }
        }
      }
    },
    configureServer(server) {
      function restartOnExperimentsFileChange(path: string) {
        if (path === EXPERIMENTS_FILE) {
          server.restart();
        }
      }

      server.watcher.on("change", restartOnExperimentsFileChange);
      server.watcher.on("add", restartOnExperimentsFileChange);
      server.watcher.on("unlink", restartOnExperimentsFileChange);
    },
  };
}
