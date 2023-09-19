const fs = require("node:fs");
const path = require("node:path");

function assertFileExists(filepath) {
  if (!fs.existsSync(filepath)) {
    throw new Error(`File ${filepath} does not exist`);
  }
}

function assertFileNotExists(filepath) {
  if (fs.existsSync(filepath)) {
    throw new Error(`File ${filepath} exists but should not`);
  }
}

try {
  assertFileExists(path.join(__dirname, "..", "pnpm-lock.yaml"));
  assertFileNotExists(path.join(__dirname, "..", "package-lock.json"));
  assertFileNotExists(path.join(__dirname, "..", "yarn.lock"));

  console.log("Lock file validation successful.");
} catch (error) {
  console.error(`Lock file validation failed: ${error.message}`);
  process.exit(1);
}
