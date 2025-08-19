import { test as setup, expect } from "@playwright/test";

const authFile = ".auth/user.json";
const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000000";

// This serves both as a test and setup for navigating the OSS login flow.
// It is used to authenticate the user and save the authentication state to a local git-ignored file.
// This stored auth state is used to skip the authentication step in subsequent tests.

setup("authenticate OSS deployment", async ({ page, request, baseURL }) => {
  // Get the base URL for API calls
  const serverHost = process.env.AIRBYTE_SERVER_HOST;
  const apiBaseUrl = serverHost ? `${serverHost}/api/v1` : `${baseURL}/api/v1`;

  try {
    // Step 1: Get workspace ID
    const workspaceResponse = await request.post(`${apiBaseUrl}/workspaces/list_by_organization_id`, {
      data: {
        organizationId: DEFAULT_ORGANIZATION_ID,
        pagination: { pageSize: 1, rowOffset: 0 },
      },
    });

    expect(workspaceResponse.ok()).toBeTruthy();
    const { workspaces } = await workspaceResponse.json();
    expect(workspaces.length).toBeGreaterThan(0);
    const workspaceId = workspaces[0].workspaceId;

    // Step 2: Complete initial setup
    const setupResponse = await request.post(`${apiBaseUrl}/workspaces/update`, {
      data: {
        workspaceId,
        initialSetupComplete: true,
        displaySetupWizard: true,
      },
    });

    expect(setupResponse.ok()).toBeTruthy();

    // Step 3: Navigate to the app to establish browser session and make sure we're using the deployed webapp
    await page.goto("/");
    await expect(page).toHaveURL(/.*localhost:3000/, { timeout: 10000 });

    console.log("✅ API authentication setup complete. Proceeding to test runs.");
  } catch (error) {
    console.error("❌ API authentication setup failed:", error);
    throw error;
  }

  // Save authenticated state for reuse
  await page.context().storageState({ path: authFile });
});
