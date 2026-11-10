import { test } from "@playwright/test";
import { signInOrRegister } from "helpers";

test("can sign in", async ({ page }) => {
  await signInOrRegister(page);
});
