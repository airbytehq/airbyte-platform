#!/usr/bin/env node

const { execSync } = require("child_process");
const fs = require("fs");

// todo: maybe support running cloud tests using frontend-dev?
const optionator = require("optionator")({
  prepend: "Usage: node run-playwright.js [options] [test-paths]",
  options: [
    {
      option: "cloud",
      alias: "c",
      type: "Boolean",
      description: "Run cloud e2e tests using cloud config.",
    },
    {
      option: "serverHost",
      type: "String",
      description: "Server host to use. defaults to frontend-dev.",
      default: "https://frontend-dev-cloud.airbyte.com",
      example: "https://frontend-dev-cloud.airbyte.com or https://local.airbyte.dev",
    },
    {
      option: "webappUrl",
      type: "String",
      description: "Webapp environment to use.",
      example: "https://localhost:3000 or https://localhost:3001",
    },
    {
      option: "debug",
      type: "Boolean",
      description: "Enable debug mode.",
    },
    {
      option: "ui",
      type: "Boolean",
      description: "Run in UI mode.",
    },
    { option: "headed", type: "Boolean", description: "Run tests in headed mode." },
    {
      option: "ci",
      type: "Boolean",
      description: "Enable CI mode.",
    },
    {
      option: "help",
      alias: "h",
      type: "Boolean",
      description: "Show help.",
    },
  ],
});

const command = ["npx", "playwright", "test"];

async function main() {
  try {
    // Filter out '--' from arguments so optionator can parse options that come after it
    const filteredArgs = process.argv.filter((arg) => arg !== "--");
    const options = optionator.parse(filteredArgs);

    if (options.help) {
      console.log(optionator.generateHelp());
      return;
    }

    // Extract any additional arguments (test paths) that weren't parsed as options
    const additionalArgs = options._;

    if (options.cloud) {
      command.push("--config=playwright.cloud-config.ts");
    }

    // Set environment variables for the test run
    const envVars = [];

    if (options.webappUrl) {
      envVars.push(`AIRBYTE_WEBAPP_URL=${options.webappUrl}`);
    }

    if (options.serverHost) {
      envVars.push(`AIRBYTE_SERVER_HOST=${options.serverHost}`);
    }

    // Prepend environment variables to the command
    if (envVars.length > 0) {
      command.unshift(...envVars);
    }

    if (options.headed) {
      command.push("--headed");
    }
    if (options.ui) {
      command.push("--ui");
    }
    if (options.debug) {
      command.push("--debug");
    }

    // Handle test path specification via additional args
    const testPaths = [];
    if (additionalArgs && additionalArgs.length > 0) {
      testPaths.push(...additionalArgs);
    }

    if (testPaths.length > 0) {
      // Validate test paths exist before running
      const invalidPaths = testPaths.filter((path) => !fs.existsSync(path));
      if (invalidPaths.length > 0) {
        console.error(`❌ Invalid test paths: ${invalidPaths.join(", ")}`);
        console.error("Please check that the paths exist and try again.");
        process.exit(1);
      }

      // When targeting specific tests, force setup to still run first
      command.push("--project=setup");
      command.push("--project=oss-acceptance");
      command.push(...testPaths);
    }

    console.log(`Running: ${command.join(" ")}`);
    execSync(command.join(" "), { stdio: "inherit", shell: true });
  } catch (error) {
    console.error(`❌ Error: ${error.message}`);
    process.exit(1);
  }
}

main();
