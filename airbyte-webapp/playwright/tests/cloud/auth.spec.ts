import { test } from "@playwright/test";
import { registerUser, signIn } from "helpers";

test("can sign in", async ({ page }) => {
  const email = process.env.CLOUD_TEST_EMAIL;
  const password = process.env.CLOUD_TEST_PASSWORD;
  if (email && password) {
    // Stage: fixed Keycloak user, so runs don't create a new account each time
    await signIn(page, email, password);
  } else {
    await page.goto("./");
    await registerUser(page);
  }
});
