/**
 * Create Connection tests. They run against the mock backend in playwright/mock.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it.
 */
import type { Page } from "@playwright/test";

import {
  completeConnectionCreation,
  filterAndFindStream,
  namespaceHelpers,
  selectSyncMode,
} from "../../helpers/connectionCreation";
import { catalogs, connection, defs, destination, expect, ids, scenario, source, test } from "../../mock";

const postgresSource = source({ def: defs.postgresSource, name: "Test Postgres source" });
const postgresDestination = destination({ def: defs.postgresDestination, name: "Test Postgres destination" });

/**
 * A connection between the Postgres source and the Postgres destination. The connections page shows the "New
 * connection" button only when the workspace has a connection.
 */
const existingConnection = connection({
  source: postgresSource,
  destination: postgresDestination,
  catalog: catalogs.postgres({ selected: true }),
});

const namespaceCheckbox = (page: Page) => page.locator('thead tr th input[data-testid="sync-namespace-checkbox"]');
const streamCheckboxes = (page: Page) =>
  page.locator('[data-testid^="row-depth-1-stream"] input[data-testid="sync-stream-checkbox"]');
const noStreamsError = (page: Page) => page.getByText("Select at least 1 stream to sync.");
const nextButton = (page: Page) => page.getByTestId("next-creation-page");

/** Checks each stream checkbox of the table: checked or not checked. */
async function expectEveryStream(page: Page, checked: boolean) {
  const checkboxes = await streamCheckboxes(page).all();
  expect.soft(checkboxes.length).toBeGreaterThan(0);
  for (const checkbox of checkboxes) {
    await expect.soft(checkbox).toBeChecked({ checked });
  }
}

test.describe(() => {
  test.use({
    airbyteScenario: scenario({
      connections: [existingConnection],
      // The asynchronous schema discovery is the default of the webapp. The mock answers it with this catalog. No
      // stream is selected, as in a source catalog that the user has not configured yet.
      outcomes: { discover: catalogs.postgres() },
    }),
  });

  test("create a connection between an existing source and destination", async ({ page, airbyte }) => {
    const { sourceId } = airbyte.store.sources.get(postgresSource);
    const { destinationId } = airbyte.store.destinations.get(postgresDestination);

    await test.step("click New connection on the connections page", async () => {
      await page.goto(`/workspaces/${airbyte.workspaceId}/connections`);
      await page.getByTestId("new-connection-button").click();

      await expect(page).toHaveURL(/\/connections\/new-connection$/);
    });

    await test.step("the webapp loads the sources and the source definitions", async () => {
      await expect.poll(() => airbyte.calls.count("listSourcesForWorkspace")).toBeGreaterThanOrEqual(1);
      await expect.poll(() => airbyte.calls.count("listSourceDefinitionsForWorkspace")).toBeGreaterThanOrEqual(1);
    });

    await test.step("the existing source option is selected", async () => {
      await expect.soft(page.getByTestId("radio-button-tile-sourceType-existing")).toBeChecked();
      await expect.soft(page.getByTestId("radio-button-tile-sourceType-new")).not.toBeChecked();
    });

    await test.step("select the existing source", async () => {
      await page.getByTestId("select-existing-source-Test Postgres source").click();

      await expect(page.getByTestId("radio-button-tile-destinationType-existing")).toBeVisible();
    });

    await test.step("the existing destination option is selected", async () => {
      await expect.soft(page.getByTestId("radio-button-tile-destinationType-existing")).toBeChecked();
      await expect.soft(page.getByTestId("radio-button-tile-destinationType-new")).not.toBeChecked();
    });

    await test.step("select the existing destination", async () => {
      await page.getByTestId("select-existing-destination-Test Postgres destination").click();

      await expect(page).toHaveURL(/\/connections\/new-connection\/configure/);
      await expect(page.locator('[data-testid^="row-depth-1-stream"]').first()).toBeVisible();
    });

    await test.step("the webapp discovers the schema of the source", async () => {
      expect.soft(airbyte.calls("runDiscoverCommand")).toContainEqual({ id: expect.any(String), actor_id: sourceId });
    });

    await test.step("no stream is selected, and the form asks for one", async () => {
      await expect.soft(namespaceCheckbox(page)).toBeEnabled();
      await expect.soft(namespaceCheckbox(page)).not.toBeChecked();
      await expectEveryStream(page, false);
      await expect.soft(noStreamsError(page)).toBeVisible();
      await expect.soft(nextButton(page)).toBeDisabled();
    });

    await test.step("the namespace row shows the namespace public, and the table controls show", async () => {
      await namespaceHelpers.verifyNamespaceRow(page, "public");
      await expect.soft(page.getByTestId("refresh-schema-btn")).toBeVisible();
      await expect.soft(page.getByTestId("expand-collapse-all-streams-btn")).toBeVisible();
    });

    await test.step("select every stream of the namespace", async () => {
      await namespaceHelpers.toggleNamespaceCheckbox(page, "public", true);

      await expect(namespaceCheckbox(page)).toBeChecked();
    });

    await test.step("every stream is selected, and the error goes away", async () => {
      await expectEveryStream(page, true);
      await expect.soft(noStreamsError(page)).toBeHidden();
      // Selected streams still need a cursor ("Cursor missing" in a later step), so the form is not complete yet.
      await expect.soft(nextButton(page)).toBeDisabled();
    });

    await test.step("the table controls still show", async () => {
      await expect.soft(page.getByTestId("refresh-schema-btn")).toBeVisible();
      await expect.soft(page.getByTestId("expand-collapse-all-streams-btn")).toBeVisible();
    });

    await test.step("clear every stream of the namespace", async () => {
      await namespaceHelpers.toggleNamespaceCheckbox(page, "public", false);

      await expect(namespaceCheckbox(page)).not.toBeChecked();
    });

    await test.step("no stream is selected", async () => {
      await expectEveryStream(page, false);
    });

    const usersRow = page.getByTestId("row-depth-1-stream-users");
    const usersCheckbox = usersRow.locator('input[data-testid="sync-stream-checkbox"]');

    await test.step("search for the users stream", async () => {
      await filterAndFindStream(page, "users");
    });

    await test.step("the namespace checkbox is disabled while the search is on, and users is not selected", async () => {
      await expect.soft(namespaceCheckbox(page)).toBeDisabled();
      await expect.soft(usersRow).toHaveClass(/disabled/);
    });

    await test.step("select the users stream", async () => {
      await usersCheckbox.check({ force: true });

      await expect(usersCheckbox).toBeChecked();
    });

    await test.step("users is selected, and it needs a cursor", async () => {
      await expect.soft(usersRow).not.toHaveClass(/disabled/);
      await expect.soft(usersRow).not.toHaveClass(/added/);
      await expect.soft(usersRow.getByText("Cursor missing")).toBeVisible();
    });

    await test.step("clear the users stream, and expand it", async () => {
      await usersCheckbox.uncheck({ force: true });
      await expect(usersCheckbox).not.toBeChecked();

      const expandButton = usersRow.locator('button[data-testid="expand-collapse-stream-btn"]');
      await expandButton.click();
      await expect(expandButton).toHaveAttribute("aria-expanded", "true");
    });

    const emailRow = page.getByTestId("row-depth-2-field-email");
    const emailCheckbox = emailRow.locator('input[data-testid="sync-field-checkbox"]');
    const idRow = page.getByTestId("row-depth-2-field-id");

    await test.step("the email field is not selected", async () => {
      await expect.soft(emailRow).toHaveClass(/disabled/);
    });

    await test.step("select the email field", async () => {
      await emailCheckbox.check({ force: true });

      await expect(emailCheckbox).toBeChecked();
    });

    await test.step("email is selected, and the id field is the primary key, which the user cannot clear", async () => {
      await expect.soft(emailRow).not.toHaveClass(/disabled/);
      await expect.soft(emailRow).not.toHaveClass(/added/);
      await expect.soft(idRow.locator('input[data-testid="sync-field-checkbox"]')).toBeDisabled();
      await expect.soft(idRow.getByTestId("field-pk-cell")).toContainText("Primary key");
    });

    await test.step("the users stream is selected too, and the namespace is partly selected", async () => {
      await expect.soft(usersCheckbox).toBeChecked();
      await expect.soft(usersRow.getByText("Cursor missing")).toBeVisible();
      await expect.soft(namespaceCheckbox(page)).toHaveAttribute("aria-checked", "mixed");
    });

    await test.step("select Full refresh | Overwrite for users", async () => {
      await selectSyncMode(page, usersRow, "Full refresh | Overwrite");
    });

    await test.step("the form is complete", async () => {
      await expect.soft(noStreamsError(page)).toBeHidden();
      await expect.soft(nextButton(page)).toBeEnabled();
    });

    await test.step("go to the next page, select the Manual schedule, and submit", async () => {
      await completeConnectionCreation(page, { scheduleType: "Manual" });
    });

    await test.step("the webapp creates the connection with the email and id fields of users, as Full refresh | Overwrite", async () => {
      await expect.poll(() => airbyte.calls.count("webBackendCreateConnection")).toBeGreaterThanOrEqual(1);
      const created = airbyte.calls("webBackendCreateConnection");
      expect.soft(created).toHaveLength(1);
      expect.soft(created[0]).toMatchObject({
        name: "Test Postgres source → Test Postgres destination",
        sourceId,
        destinationId,
        // The ID of the catalog that the discovery found. The existing connection has the first catalog ID.
        sourceCatalogId: ids.catalog(2),
        scheduleType: "manual",
      });
      expect.soft(created[0]?.syncCatalog?.streams).toContainEqual(
        expect.objectContaining({
          stream: expect.objectContaining({ namespace: "public", name: "users" }),
          config: expect.objectContaining({
            selected: true,
            syncMode: "full_refresh",
            destinationSyncMode: "overwrite",
            fieldSelectionEnabled: true,
            selectedFields: [{ fieldPath: ["email"] }, { fieldPath: ["id"] }],
          }),
        })
      );
      const selected = created[0]?.syncCatalog?.streams.filter(({ config }) => config?.selected);
      expect.soft(selected).toHaveLength(1);
    });

    await test.step("the Status tab of the new connection opens", async () => {
      // The existing connection has the first connection ID.
      await expect(page).toHaveURL(new RegExp(`/connections/${ids.connection(2)}/status`));
    });

    await test.step("reload the page", async () => {
      await page.reload();
    });

    await test.step("the new connection is pending, because it has never synced", async () => {
      await expect.soft(page.getByTestId("connection-status-indicator")).toHaveAttribute("data-status", "pending");
    });
  });
});
