import { pendingConnectionScenario, manualSyncScenario } from "./status.scenarios";
import { connectionUI } from "../../helpers/connection";
import { test, expect } from "../../helpers/frontendTest";

test.describe("Connection Status - mocked API", () => {
  test.describe("Pending connection", () => {
    test.use({ airbyteScenario: pendingConnectionScenario });

    test("renders pending status returned by the API", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await connectionUI.visit(page, connection, "status");
      await expect(page.getByTestId("connection-status-indicator")).toHaveAttribute("data-status", "pending");
      expect(airbyte.requests).toContainEqual({
        key: "POST /api/v1/connections/status",
        body: { connectionIds: [connection.connectionId] },
      });
    });
  });

  test.describe("Manual sync", () => {
    test.use({ airbyteScenario: manualSyncScenario });

    test("starts and cancels a manual sync, preserving cancellation after reload", async ({ page, airbyte }) => {
      const connection = airbyte.connections[0];
      await connectionUI.visit(page, connection, "status");
      await expect(page.getByTestId("manual-sync-button")).toBeEnabled();

      await connectionUI.startManualSync(page);
      await expect(page.getByTestId("connection-status-indicator")).toHaveAttribute("data-status", "running");
      await expect(page.getByTestId("connection-status-indicator")).toHaveAttribute("data-loading", "true");
      await expect(page.getByTestId("streams-list-subtitle")).toContainText("Sync starting");
      await expect(page.getByTestId("manual-sync-button")).toBeHidden();
      await expect(page.getByTestId("cancel-sync-button")).toBeEnabled();
      expect(airbyte.requests).toContainEqual({
        key: "POST /api/v1/connections/sync",
        body: { connectionId: connection.connectionId },
      });
      expect(airbyte.jobs.list()).toHaveLength(1);
      const job = airbyte.jobs.latestForConnection(connection.connectionId)!;
      expect(job).toMatchObject({ configId: connection.connectionId, configType: "sync", status: "running" });

      await connectionUI.cancelSync(page);
      await expect(page.getByTestId("connection-status-indicator")).toHaveAttribute("data-status", "incomplete");
      await expect(page.getByTestId("manual-sync-button")).toBeEnabled();
      expect(airbyte.requests).toContainEqual({ key: "POST /api/v1/jobs/cancel", body: { id: job.id } });
      expect(airbyte.jobs.latestForConnection(connection.connectionId)).toMatchObject({
        id: job.id,
        status: "cancelled",
      });
      expect(airbyte.connections[0].isSyncing).toBe(false);
      expect(job.status).toBe("running");

      await page.reload();
      await expect(page.getByTestId("connection-status-indicator")).toHaveAttribute("data-status", "incomplete");
      await expect(page.getByTestId("manual-sync-button")).toBeEnabled();
      await expect(page.getByTestId("cancel-sync-button")).toBeHidden();
    });
  });
});
