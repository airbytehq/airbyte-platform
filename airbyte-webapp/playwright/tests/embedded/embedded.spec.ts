import { spawn } from "child_process";
import fs from "fs";
import path from "path";

import { test, expect } from "@playwright/test";
import { signIn } from "helpers";

/**
 * This test suite covers the end-to-end flow of creating connection templates,
 * creating a source config template, and running the embedded widget flow with
 * an application that has copied environment variables.
 * It uses Playwright to automate the browser interactions and spawn a local
 * Express server to handle the embedded widget.
 * The server is started with environment variables copied from the Airbyte
 * Applications UI, allowing the embedded widget to connect to the Airbyte API.
 * The test suite includes:
 * - Creating an application and copying environment variables
 * - Running the embedded widget flow with the copied environment variables
 * - Interacting with the embedded widget to configure a connection
 * - Validating the success of the embedded widget flow
 */

const EMBEDDED_ENV_PATH = path.join(__dirname, ".embedded-env.json");
let copiedEnvVars: Record<string, string> = {};
let serverProcess: ReturnType<typeof spawn>;

test.describe("Airbyte Embedded Integration Flow", () => {
  test.setTimeout(5 * 60 * 1000); // Set timeout to 5 minutes for all tests in this suite

  test.beforeEach(async ({ page }) => {
    // Log server errors
    page.on("response", async (response) => {
      if (response.status() >= 400) {
        const url = response.url();
        let body;
        try {
          body = await response.text();
        } catch {
          body = "<unable to read body>";
        }
        console.error(`[SERVER ERROR] ${response.status()} ${url}\n${body}`);
      }
    });
  });

  test("should create an application and write env vars to file", async ({ page }) => {
    // // Create application and copy env vars
    await signIn(page, process.env.USER_EMAIL, process.env.USER_PASSWORD);
    await page.click('a:has-text("Settings")');
    await page.click('a:has-text("Embedded")');
    // Grant clipboard-read and clipboard-write permissions before clicking Copy
    await page.context().grantPermissions(["clipboard-read", "clipboard-write"], { origin: page.url() });

    // Ensure the page is focused before copying
    await page.bringToFront();

    await page.click('button:has-text("Copy")');
    // Wait briefly to ensure clipboard is updated
    await page.waitForTimeout(300);

    const copiedText = await page.evaluate(() => navigator.clipboard.readText());

    // parse as .env format (key=value per line)
    const envObj: Record<string, string> = {};
    copiedText.split("\n").forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) {
        return;
      }
      const eqIdx = trimmed.indexOf("=");
      if (eqIdx > 0) {
        const key = trimmed.slice(0, eqIdx).trim();
        const value = trimmed.slice(eqIdx + 1).trim();
        envObj[key] = value;
      }
    });
    copiedEnvVars = envObj;
    // Write to local JSON file
    fs.writeFileSync(EMBEDDED_ENV_PATH, JSON.stringify(copiedEnvVars, null, 2), { encoding: "utf-8" });
  });

  test("should run embedded widget flow using env vars from file", async ({ page }) => {
    // Read env vars from file
    if (!fs.existsSync(EMBEDDED_ENV_PATH)) {
      throw new Error(`Env file ${EMBEDDED_ENV_PATH} does not exist. Run the application setup test first.`);
    }
    copiedEnvVars = JSON.parse(fs.readFileSync(EMBEDDED_ENV_PATH, "utf-8"));

    // Ensure AIRBYTE_SERVER_HOST is defined
    if (!process.env.AIRBYTE_SERVER_HOST) {
      throw new Error("AIRBYTE_SERVER_HOST is not defined in process.env. Cannot set BASE_URL.");
    }

    // Start the Express server with env vars from Applications UI + extra
    const extraEnvs = {
      EXTERNAL_USER_ID: "test-external-user",
      BASE_URL: `${process.env.AIRBYTE_SERVER_HOST}/api/public`,
    };
    const serverPath = path.join(__dirname, "../../support/embedded-server.js");
    serverProcess = spawn("node", [serverPath], {
      env: { ...process.env, ...copiedEnvVars, ...extraEnvs, PORT: "4000" },
      stdio: ["ignore", "pipe", "pipe"],
    });

    serverProcess.stdout?.on("data", (data) => {
      console.log(`[server stdout]: ${data.toString()}`);
    });
    serverProcess.stderr?.on("data", (data) => {
      console.error(`[server stderr]: ${data.toString()}`);
    });

    await new Promise((resolve) => setTimeout(resolve, 1500));

    // Prepare a promise that resolves when EMBEDDED_RESPONSE_RECEIVED is seen in stdout
    const waitForEmbeddedResponse = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () => {
          serverProcess.stdout?.off("data", onData);
          reject(new Error("Timeout waiting for embedded response log"));
        },
        5 * 60 * 1000
      ); // 5 minutes

      function onData(data: Buffer) {
        if (data.toString().includes("EMBEDDED_RESPONSE_RECEIVED")) {
          clearTimeout(timeout);
          serverProcess.stdout?.off("data", onData);
          resolve();
        }
      }
      serverProcess.stdout?.on("data", onData);
    });

    // Run the embedded widget test
    const apiBase = "http://localhost:4000";
    const indexHtmlUrl = `http://localhost:4000/index.html?apiBase=${encodeURIComponent(apiBase)}`;
    await page.goto(indexHtmlUrl);
    await page.click("button#open-widget");
    await page.waitForSelector('iframe[src*="airbyte"]', { timeout: 10 * 1000 });
    const widgetIframe = await page.$('iframe[src*="airbyte"]');
    expect(widgetIframe).not.toBeNull();

    // Interact with content inside the iframe
    const frame = await widgetIframe!.contentFrame();
    await frame?.waitForSelector('button:has-text("Faker")', { timeout: 30 * 1000 });
    await frame?.click('button:has-text("Faker")');
    await frame?.click('button:has-text("Save")');

    // Wait for the server to log EMBEDDED_RESPONSE_RECEIVED to stdout
    await waitForEmbeddedResponse;
  });

  test("should delete workspace", async ({ page }) => {
    await signIn(page, process.env.USER_EMAIL, process.env.USER_PASSWORD);
    await page.click('a:has-text("test-external-user")');
    await page.click('a:has-text("Settings")');
    await page.click('a[href*="/settings/workspace"]');

    // wait for cookie banner to disappear
    await page.waitForTimeout(6000);

    await page.click('button:has-text("Delete your workspace")');
    await page.locator('input[id="confirmation-text"]').fill("test-external-user");
    await page.click('button:has-text("Delete workspace")');
  });
  // eslint-disable-next-line no-empty-pattern
  test.afterAll(async ({}, testInfo) => {
    if (serverProcess) {
      serverProcess.kill();
    }
    // only delete env file after final retry
    if (testInfo.retry === testInfo.project.retries) {
      if (fs.existsSync(EMBEDDED_ENV_PATH)) {
        fs.unlinkSync(EMBEDDED_ENV_PATH);
      }
    }
  });
});
