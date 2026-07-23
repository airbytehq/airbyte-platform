import { test } from "@playwright/test";
import { registerUser } from "helpers";

test("can sign in", async ({ page }) => {
  await page.goto("./");
  await registerUser(page);
});
