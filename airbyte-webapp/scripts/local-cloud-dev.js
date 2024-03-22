const fs = require("fs");
const path = require("path");

const chalk = require("chalk");
const dotenv = require("dotenv");

if (!process.env.CLOUD_ENV) {
  return;
}

const envFile = path.resolve(__dirname, "../../../cloud/cloud-webapp/development", `.env.${process.env.CLOUD_ENV}`);

if (!fs.existsSync(envFile)) {
  console.error(
    `${chalk.bold.inverse.red(
      "This mode is for Airbyte employees only and doesn't work in the airbyte-platform repository."
    )}\n` +
      `Could not find .env file for environment ${process.env.CLOUD_ENV} (looking for ${chalk.bold.gray(envFile)}).\n` +
      `You can only run this command from the ${chalk.green("airbyte-platform-internal")} repository.\n`
  );
  process.exit(42);
}

process.env.REACT_APP_CLOUD = "true";

dotenv.config({ path: envFile });
