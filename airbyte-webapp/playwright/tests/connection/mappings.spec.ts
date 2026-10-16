/**
 * Connection Mappings Tests
 *
 * Tests the Mappings tab of a connection: the upsell when the plan does not include mappings, and the first mapping
 * of a stream when it does.
 *
 * Every test runs against the mock backend in playwright/mock. Each test starts from the backend state in its own
 * scenario, so no test depends on what an earlier test did. The scenario sets the mappings features of the webapp.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it. The check for the "saved"
 * notification stays a normal `expect`, because the checks of the saved request need the save to be finished.
 */

import type { FieldSpec } from "@src/core/api/types/AirbyteClient";

import { FeatureItem } from "@src/core/services/features/types";

import { catalogs, connection, defs, destination, expect, mapperValidation, scenario, source, test } from "../../mock";

/**
 * A Postgres → Postgres connection with every stream selected. Its catalog has the streams cars, cities and users in
 * the namespace "public" (createMockPostgresCatalog in helpers/mocks.ts).
 */
const postgresConnection = connection({
  source: source({ def: defs.postgresSource, name: "Postgres source" }),
  destination: destination({ def: defs.postgresDestination, name: "Postgres destination" }),
  catalog: catalogs.postgres({ selected: true }),
});

/** The fields of the users stream, as the server reports them for mapper validation. id is the primary key. */
const usersFields: FieldSpec[] = [
  { name: "email", type: "STRING", isSelectedPrimaryKey: false, isSelectedCursor: false },
  { name: "id", type: "INTEGER", isSelectedPrimaryKey: true, isSelectedCursor: false },
  { name: "name", type: "STRING", isSelectedPrimaryKey: false, isSelectedCursor: false },
  { name: "updated_at", type: "TIMESTAMP_WITHOUT_TIMEZONE", isSelectedPrimaryKey: false, isSelectedCursor: false },
];

/**
 * Mappings not in the plan
 *
 * The start state is the Postgres connection without mappings, in a workspace whose plan does not include mappings.
 */
test.describe(() => {
  test.use({
    airbyteScenario: scenario({ connections: [postgresConnection], features: { [FeatureItem.MappingsUI]: false } }),
  });

  test("see how to unlock mappings when the plan does not include them", async ({ page, airbyte }) => {
    await test.step("open the Mappings tab", async () => {
      await page.goto(airbyte.paths.connection(postgresConnection, "mappings"));
    });

    await test.step("the upsell shows in place of the mapping form", async () => {
      await expect.soft(page.getByTestId("mappings-upsell-empty-state")).toBeVisible();
      await expect.soft(page).toHaveURL(/\/mappings$/);
      await expect.soft(page.getByTestId("add-stream-for-mapping-combobox")).toHaveCount(0);
    });
  });
});

/**
 * Mappings in the plan
 *
 * The start state is the Postgres connection without mappings, in a workspace whose plan includes mappings and
 * advanced mappings. With advanced mappings, a new mapping hashes a field.
 */
test.describe(() => {
  test.use({
    airbyteScenario: scenario({
      connections: [postgresConnection],
      features: { [FeatureItem.MappingsUI]: true, [FeatureItem.AdvancedMappingsUI]: true },
    }),
  });

  test("add a first mapping that hashes a field of a stream", async ({ page, airbyte }) => {
    const { connectionId } = airbyte.store.connections.get(postgresConnection);
    // The field dropdown lists the fields from the mapper validation answer. The mock does not know the fields of a
    // stream, so the test gives them.
    airbyte.override("webBackendValidateMappers", mapperValidation.allValid(usersFields));
    const fieldDropdown = page.getByPlaceholder("Select a field");

    await test.step("open the Mappings tab", async () => {
      await page.goto(airbyte.paths.connection(postgresConnection, "mappings"));
      await expect(page.getByTestId("add-stream-for-mapping-combobox")).toBeVisible();
    });

    await test.step("add a mapping for the users stream", async () => {
      await page.getByTestId("add-stream-for-mapping-combobox").click();
      await page.getByRole("option", { name: "users" }).click();
      // The webapp disables the field until it has validated the new mapping.
      await expect.poll(() => airbyte.calls("webBackendValidateMappers").length).toBeGreaterThan(0);
      await expect(fieldDropdown).toBeEnabled();
    });

    await test.step("the new mapping has no field yet", async () => {
      await expect.soft(fieldDropdown).toHaveValue("");
    });

    await test.step("hash the field name", async () => {
      await fieldDropdown.click();
      await page.getByRole("option", { name: "name" }).click();
      // The webapp validates the mapping again with the new field, and disables the field until the answer is in.
      await expect
        .poll(() => airbyte.calls.last("webBackendValidateMappers"))
        .toMatchObject({ mappers: [{ mapperConfiguration: { targetField: "name" } }] });
      await expect(fieldDropdown).toBeEnabled();
    });

    await test.step("the validation request hashes the field name of the users stream", async () => {
      await expect.soft(fieldDropdown).toHaveValue("name");
      expect.soft(airbyte.calls.last("webBackendValidateMappers")).toMatchObject({
        connectionId,
        streamDescriptor: { namespace: "public", name: "users" },
        mappers: [{ type: "hashing", mapperConfiguration: { targetField: "name" } }],
      });
    });

    await test.step("save the mapping", async () => {
      await page.getByTestId("submit-mappings").click();
      // The destination supports refreshes, so the webapp recommends a refresh. Keep the default choice, refresh the
      // stream. The update request then has skipReset: false.
      await page.getByTestId("refreshModal-save").click();
      await expect(page.getByTestId("notification-connection_settings_change_success")).toBeVisible();
    });

    await test.step("the update request saves a SHA-256 hashing mapper on name", async () => {
      const updates = airbyte.calls("webBackendUpdateConnection");
      expect.soft(updates).toHaveLength(1);
      expect.soft(updates[0]).toMatchObject({ connectionId, skipReset: false });
      expect.soft(catalogs.find(updates[0]?.syncCatalog, "public", "users").config?.mappers).toEqual([
        {
          id: expect.any(String),
          type: "hashing",
          mapperConfiguration: { targetField: "name", method: "SHA-256", fieldNameSuffix: "_hashed" },
        },
      ]);
    });
  });
});
