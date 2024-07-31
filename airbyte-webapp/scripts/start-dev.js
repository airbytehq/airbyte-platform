const origSpawn = require("node:child_process").spawn;
const { existsSync } = require("node:fs");
const path = require("node:path");

const { default: select, Separator } = require("@inquirer/select");
const chalk = require("chalk");
const groupBy = require("lodash/groupBy");

const environments = require("../environments.json");

/**
 * Wrapper around node's `spawn` function to promisify it.
 * Note: We don't use `node:utils/promisify` because it would exit the parent process
 * when the child process exits, while we just want to resolve the promise instead.
 */
const spawn = (cmd, args, options) => {
  return new Promise((resolve, reject) => {
    const child = origSpawn(cmd, args, { stdio: "inherit", ...options });
    child.on("exit", (code) => {
      resolve(code);
    });
    child.on("close", (code) => {
      resolve(code);
    });
    child.on("error", (err) => {
      reject(err);
    });
  });
};

const areCloudEnvsAvailable = existsSync(path.join(__dirname, "../../../cloud/cloud-webapp/development"));

const groupedEnvs = groupBy(
  Object.entries(environments)
    .filter(([, entry]) => {
      if (entry.cloudEnv && !areCloudEnvsAvailable) {
        // Only show cloud environments if we're running in airbyte-platform-internal
        return false;
      }
      if (entry.envFile && !existsSync(path.join(__dirname, "..", entry.envFile))) {
        // If environment requires an additional env file only show it the file exists
        return false;
      }
      return true;
    })
    .map(([id, entry]) => ({ id, ...entry })),
  "group"
);

const choices = Object.entries(groupedEnvs).flatMap(([group, entries]) => {
  return [
    new Separator(`--- ${group} ---`),
    ...entries.map((entry) => ({
      name: entry.name,
      value: entry.id,
      description: entry.description,
    })),
  ];
});

async function preStart() {
  await spawn("pnpm", ["generate-client"], { stdio: "inherit" });
}

async function main() {
  try {
    const selectedEnv =
      process.argv[2]?.replace(/^--/, "") ||
      (await select({
        message: "Select your backend environment to develop against:",
        choices,
        pageSize: 15,
      }));

    console.log(`Selected environment: ${chalk.bold.cyan(selectedEnv)}`);

    const env = environments[selectedEnv];
    if (!env) {
      throw new Error(`Invalid environment selected: ${selectedEnv}`);
    }

    console.log(
      `\n💡 Run ${chalk.cyan(`pnpm start --${selectedEnv}`)} to run against this environment without a select prompt.`
    );

    await preStart();

    console.log("\n> pnpm vite\n");
    await spawn("pnpm", ["vite"], {
      stdio: "inherit",
      env: {
        ...process.env,
        ...(env.cloudEnv ? { NODE_OPTIONS: "-r ./scripts/local-cloud-dev.js", CLOUD_ENV: env.cloudEnv } : {}),
        ...(env.envFile
          ? { NODE_OPTIONS: "-r dotenv/config", DOTENV_CONFIG_PATH: path.join(__dirname, "..", env.envFile) }
          : {}),
      },
    });
  } catch (e) {
    if (e.name !== "ExitPromptError") {
      throw e;
    }
  }
}

main();
