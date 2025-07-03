import { test } from "@playwright/test";
import { signInOrRegister, signOut } from "helpers";

test("can sign in", async ({ page }) => {
  await page.goto("./");
  await signInOrRegister(page);
  await signOut(page);
});
