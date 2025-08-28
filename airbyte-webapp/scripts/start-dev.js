const origSpawn = require("node:child_process").spawn;

const { default: select } = require("@inquirer/select");
const chalk = require("chalk");
const optionator = require("optionator")({
  options: [
    {
      option: "preview",
      alias: "p",
      type: "Boolean",
      description: "Run the webapp in a production build using `vite preview`.",
    },
  ],
});

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

const choices = Object.entries(environments).flatMap(([id, entry]) => {
  return [
    {
      name: entry.name,
      apiUrl: entry.apiUrl,
      description: entry.description,
      value: id,
    },
  ];
});

async function preStart() {
  await spawn("./scripts/clean-generated.sh", [], { stdio: "inherit" });
  await spawn("pnpm", ["generate-client"], { stdio: "inherit" });
}

async function main() {
  try {
    const options = optionator.parse(process.argv);

    const selectedEnv =
      options._[0] ||
      (await select({
        message: "Select your backend environment to develop against:",
        choices,
        pageSize: 15,
      }));
    if (options.preview) {
      console.log(`Running as preview against environment: ${chalk.bold.cyan(selectedEnv)}`);
    } else {
      console.log(`Selected environment: ${chalk.bold.cyan(selectedEnv)}`);
    }

    const env = environments[selectedEnv];
    if (!env) {
      throw new Error(`Invalid environment selected: ${selectedEnv}`);
    }

    console.log(
      `\n💡 Run ${chalk.cyan(
        `pnpm start ${!!options.preview ? "--preview " : ""}${selectedEnv}`
      )} to run against this environment without a select prompt.`
    );

    await preStart();

    console.log(env.apiUrl);

    if (options.preview) {
      console.log(`\n> pnpm vite build && pnpm vite preview --port "3000"}\n`);
      await spawn("pnpm", ["vite", "build"], {
        stdio: "inherit",
        env: {
          ...process.env,
          REACT_APP_API_URL: process.env.REACT_APP_API_URL ?? env.apiUrl,
          REACT_APP_KEYCLOAK_BASE_URL: env.apiUrl,
        },
      });
      await spawn("pnpm", ["vite", "preview", "--port", `"3000"`], {
        stdio: "inherit",
      });
    } else {
      console.log("\n> pnpm vite\n");
      await spawn("pnpm", ["vite"], {
        stdio: "inherit",
        env: {
          ...process.env,
          REACT_APP_API_URL: process.env.REACT_APP_API_URL ?? env.apiUrl,
          REACT_APP_KEYCLOAK_BASE_URL: env.apiUrl,
        },
      });
    }
  } catch (e) {
    if (e.name !== "ExitPromptError") {
      throw e;
    }
  }
}

main();
