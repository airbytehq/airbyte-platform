/**
 * Auto-Detect Schema Changes Tests
 *
 * Tests how a connection shows and handles changes in the schema of its source: no changes, non-breaking changes,
 * and breaking changes. Also tests the connection setting that says what to do with non-breaking changes.
 *
 * Every test runs against the mock backend in playwright/mock. Each test starts from the backend state in its own
 * scenario, so no test depends on what an earlier test did.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it.
 *
 * The mock does not compute schema diffs. When a test refreshes the schema, `airbyte.schema.onRefresh` supplies what
 * the server found: the refreshed catalog, the diff, and the kind of schema change.
 */

import type { ConnectionHandle, MockAirbyte, SchemaRefreshAnswer } from "../../mock";
import type { Page } from "@playwright/test";
import type { AirbyteCatalog } from "@src/core/api/types/AirbyteClient";

import { SyncMode, DestinationSyncMode } from "@src/core/api/types/AirbyteClient";

import { connectionForm } from "../../helpers/connection";
import { connectionList } from "../../helpers/connectionList";
import { schemaChange, catalogDiffModal, streamsTable, replicationForm } from "../../helpers/replication";
import { catalogs, connection, defs, destination, expect, scenario, source, test } from "../../mock";

const postgresSource = source({ def: defs.postgresSource, name: "Postgres source" });
const postgresDestination = destination({ def: defs.postgresDestination, name: "Postgres destination" });

/**
 * Every scenario in this file uses the synchronous schema refresh, because airbyte.schema.onRefresh answers only that
 * one.
 */
const experiments = { asyncSchemaDiscovery: false };

/** Waits until the replication page shows the refresh schema button, which the tests click or check next. */
async function waitForReplicationPage(page: Page) {
  await expect(page.locator('button[data-testid="refresh-schema-btn"]')).toBeVisible();
}

/** Opens the replication page of a connection and waits for it to load. */
async function openReplicationPage(page: Page, airbyte: MockAirbyte, ref: ConnectionHandle) {
  await page.goto(airbyte.paths.connection(ref, "replication"));
  await waitForReplicationPage(page);
}

/** The switch on the connection page that turns the connection on and off. */
const connectionSwitch = (page: Page) => page.locator('[data-testid="connection-status-switch"]');

/**
 * Finds a stream by name in a catalog, for example in a request body. Returns undefined when the stream is missing, so
 * that a soft check fails and the test goes on.
 */
const findStream = (catalog: AirbyteCatalog | undefined, name: string) =>
  catalog?.streams.find(({ stream }) => stream?.name === name);

/**
 * No changes
 *
 * The start state is a Postgres → Postgres connection with every stream selected. The source schema has not changed
 * since the connection was made.
 */
const unchangedConnection = connection({
  source: postgresSource,
  destination: postgresDestination,
  catalog: catalogs.postgres({ selected: true }),
});

const unchangedScenario = scenario({ connections: [unchangedConnection], experiments });

/** A refresh of a source schema that has not changed. */
const noChanges: SchemaRefreshAnswer = {
  catalog: catalogs.postgres({ selected: true }),
  diff: { transforms: [] },
  schemaChange: "no_change",
};

test.describe(() => {
  test.use({ airbyteScenario: unchangedScenario });

  test("refresh the schema when the source has not changed", async ({ page, airbyte }) => {
    const unchanged = airbyte.store.connections.get(unchangedConnection);
    airbyte.schema.onRefresh(unchangedConnection, noChanges);

    await test.step("refresh the schema", async () => {
      await openReplicationPage(page, airbyte, unchangedConnection);

      await streamsTable.clickRefreshSchemaButton(page);

      await schemaChange.verifyNoDiffToast(page);
      expect.soft(airbyte.calls.last("webBackendGetConnection")).toEqual({
        connectionId: unchanged.connectionId,
        withRefreshedCatalog: true,
      });
    });

    await test.step("no changes show", async () => {
      await catalogDiffModal.verifyModalNotVisible(page);
    });
  });
});

/**
 * Non-breaking schema change
 *
 * The start state is a Postgres → Postgres connection made when the source had the tables cities and users. The
 * source has since changed:
 * - the table cities is gone
 * - the table cars is new
 * - users lost the column email and has the new columns address and phone
 *
 * An earlier schema refresh found these non-breaking changes. The connection's preference for non-breaking changes is
 * "ignore", so the server kept the connection enabled.
 */
const nonBreakingConnection = connection({
  source: postgresSource,
  destination: postgresDestination,
  catalog: catalogs.postgres({ selected: true, withoutStreams: ["cars"] }),
  schemaChange: "non_breaking",
  nonBreakingChangesPreference: "ignore",
});

const nonBreakingChangeScenario = scenario({ connections: [nonBreakingConnection], experiments });

/**
 * A refresh of the changed source. The server keeps the config of the streams it already had, and adds cars as a
 * stream that is not selected.
 */
const sourceChanged: SchemaRefreshAnswer = {
  catalog: catalogs.postgres({
    selected: true,
    streams: { cars: { selected: false } },
    withoutStreams: ["cities"],
    withoutFields: { users: ["email"] },
    withFields: { users: { address: { type: "string" }, phone: { type: "string" } } },
  }),
  diff: {
    transforms: [
      { transformType: "remove_stream", streamDescriptor: { namespace: "public", name: "cities" } },
      { transformType: "add_stream", streamDescriptor: { namespace: "public", name: "cars" } },
      {
        transformType: "update_stream",
        streamDescriptor: { namespace: "public", name: "users" },
        updateStream: {
          fieldTransforms: [
            {
              transformType: "remove_field",
              fieldName: ["email"],
              breaking: false,
              removeField: { schema: { type: "string" } },
            },
            {
              transformType: "add_field",
              fieldName: ["address"],
              breaking: false,
              addField: { schema: { type: "string" } },
            },
            {
              transformType: "add_field",
              fieldName: ["phone"],
              breaking: false,
              addField: { schema: { type: "string" } },
            },
          ],
          streamAttributeTransforms: [],
        },
      },
    ],
  },
  schemaChange: "non_breaking",
};

/** A refresh after the source is back to the schema the connection was made from. */
const sourceRestored: SchemaRefreshAnswer = {
  catalog: catalogs.postgres({ selected: true, withoutStreams: ["cars"] }),
  diff: { transforms: [] },
  schemaChange: "no_change",
};

test.describe("Non-breaking schema change", () => {
  test.describe(() => {
    test.use({ airbyteScenario: nonBreakingChangeScenario });

    test("review and save a non-breaking schema change", async ({ page, airbyte }) => {
      const changed = airbyte.store.connections.get(nonBreakingConnection);
      airbyte.schema.onRefresh(nonBreakingConnection, sourceChanged);
      // The server saves the refreshed catalog with the connection. The saved catalog then matches the source schema,
      // so the server reports no schema change any more. The mock does not compute this, so the override supplies it.
      // next() runs the normal mock save first.
      airbyte.override("webBackendUpdateConnection", ({ body, next, store }) => {
        next();
        return store.connections.update(body.connectionId, { schemaChange: "no_change" });
      });

      await test.step("the connection list shows the connection enabled and without a warning", async () => {
        await connectionList.visit(page, airbyte.workspaceId);

        await connectionList.verifyNoWarningIcon(page, changed);
        await connectionList.verifyConnectionEnabled(page, changed);
      });

      await test.step("open the replication page", async () => {
        await openReplicationPage(page, airbyte, nonBreakingConnection);
      });

      await test.step("the non-breaking change banner shows, and the connection stays enabled", async () => {
        await schemaChange.verifySchemaChangeBanner(page, false);
        await expect.soft(connectionSwitch(page)).toBeChecked();
        await expect.soft(connectionSwitch(page)).toBeEnabled();
      });

      await test.step("review the changes", async () => {
        await schemaChange.clickReviewChangesButton(page);

        await catalogDiffModal.verifyModalVisible(page);
        expect.soft(airbyte.calls.last("webBackendGetConnection")).toEqual({
          connectionId: changed.connectionId,
          withRefreshedCatalog: true,
        });
      });

      await test.step("the changes show the removed and new streams and fields", async () => {
        await catalogDiffModal.verifyRemovedStreams(page, ["cities"]);
        await catalogDiffModal.verifyNewStreams(page, ["cars"]);
        await catalogDiffModal.toggleStreamAccordion(page, "users");
        await catalogDiffModal.verifyRemovedFields(page, ["email"]);
        await catalogDiffModal.verifyNewFields(page, ["address", "phone"]);
      });

      await test.step("close the changes, and the banner goes away", async () => {
        await catalogDiffModal.closeModal(page);

        await schemaChange.verifySchemaChangeBannerCleared(page);
      });

      await test.step("save the refreshed streams", async () => {
        await replicationForm.saveChanges(page, false);

        const update = airbyte.calls.last("webBackendUpdateConnection");
        expect.soft(update).toMatchObject({ connectionId: changed.connectionId, skipReset: true });
        expect.soft(update?.syncCatalog?.streams.map(({ stream }) => stream?.name).sort()).toEqual(["cars", "users"]);
        expect
          .soft(Object.keys(findStream(update?.syncCatalog, "users")?.stream?.jsonSchema?.properties ?? {}))
          .toEqual(["updated_at", "name", "id", "address", "phone"]);
        expect.soft(findStream(update?.syncCatalog, "cars")?.config?.selected).toBe(false);
      });

      await test.step("the connection stays enabled, and the banner stays away", async () => {
        await expect.soft(connectionSwitch(page)).toBeChecked();
        await expect.soft(connectionSwitch(page)).toBeEnabled();
        await schemaChange.verifySchemaChangeBannerCleared(page);
      });
    });
  });

  test.describe(() => {
    test.use({ airbyteScenario: nonBreakingChangeScenario });

    test("clear a non-breaking schema change after the source is restored", async ({ page, airbyte }) => {
      const changed = airbyte.store.connections.get(nonBreakingConnection);
      airbyte.schema.onRefresh(nonBreakingConnection, sourceRestored);

      await test.step("the non-breaking change banner shows", async () => {
        await openReplicationPage(page, airbyte, nonBreakingConnection);

        await schemaChange.verifySchemaChangeBanner(page, false);
      });

      await test.step("review the changes", async () => {
        await schemaChange.clickReviewChangesButton(page);

        await schemaChange.verifyNoDiffToast(page);
        expect.soft(airbyte.calls.last("webBackendGetConnection")).toEqual({
          connectionId: changed.connectionId,
          withRefreshedCatalog: true,
        });
      });

      await test.step("the banner goes away, and no changes show", async () => {
        await schemaChange.verifySchemaChangeBannerCleared(page);
        await catalogDiffModal.verifyModalNotVisible(page);
      });
    });
  });
});

/**
 * Non-breaking schema update preference
 *
 * The start state is `unchangedConnection`. Its preference for non-breaking changes is "ignore", the default of the
 * mock store.
 */
test.describe(() => {
  test.use({ airbyteScenario: unchangedScenario });

  test("change what the connection does with non-breaking schema changes", async ({ page, airbyte }) => {
    const unchanged = airbyte.store.connections.get(unchangedConnection);

    await test.step("select disable in the advanced settings", async () => {
      await page.goto(airbyte.paths.connection(unchangedConnection, "settings"));
      await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();

      await connectionForm.toggleAdvancedSettings(page);
      await replicationForm.selectNonBreakingChangesPreference(page, "disable");
    });

    await test.step("save the settings", async () => {
      await page.getByRole("button", { name: "Save changes" }).click();

      await expect(page.getByText("Your changes were saved!")).toBeVisible();
      expect.soft(airbyte.calls.last("webBackendUpdateConnection")).toMatchObject({
        connectionId: unchanged.connectionId,
        nonBreakingChangesPreference: "disable",
      });
    });
  });
});

/**
 * Breaking schema change
 *
 * The start state is a Postgres → Postgres connection whose users stream syncs incrementally with the cursor
 * updated_at. The source has since dropped the column updated_at. An earlier schema refresh found this breaking
 * change, and the server disabled the connection.
 */

/** users as Incremental | Append + Deduped with the cursor updated_at. */
const incrementalUsers = {
  syncMode: SyncMode.incremental,
  destinationSyncMode: DestinationSyncMode.append_dedup,
  cursorField: ["updated_at"],
};

const brokenConnection = connection({
  source: postgresSource,
  destination: postgresDestination,
  catalog: catalogs.postgres({ selected: true, streams: { users: incrementalUsers } }),
  status: "inactive",
  schemaChange: "breaking",
});

const breakingChangeScenario = scenario({ connections: [brokenConnection], experiments });

/** A refresh while updated_at is missing: users has lost the field, and the server marks it as a breaking change. */
const updatedAtDropped: SchemaRefreshAnswer = {
  catalog: catalogs.postgres({
    selected: true,
    streams: { users: incrementalUsers },
    withoutFields: { users: ["updated_at"] },
  }),
  diff: {
    transforms: [
      {
        transformType: "update_stream",
        streamDescriptor: { namespace: "public", name: "users" },
        updateStream: {
          fieldTransforms: [
            {
              transformType: "remove_field",
              fieldName: ["updated_at"],
              breaking: true,
              removeField: {
                schema: { type: "string", format: "date-time", airbyte_type: "timestamp_without_timezone" },
              },
            },
          ],
          streamAttributeTransforms: [],
        },
      },
    ],
  },
  schemaChange: "breaking",
};

/** A refresh after updated_at is back: the source schema is the one the connection was made from. */
const updatedAtRestored: SchemaRefreshAnswer = {
  catalog: catalogs.postgres({ selected: true, streams: { users: incrementalUsers } }),
  diff: { transforms: [] },
  schemaChange: "no_change",
};

test.describe("Breaking schema change", () => {
  test.describe(() => {
    test.use({ airbyteScenario: breakingChangeScenario });

    test("find and fix a breaking schema change", async ({ page, airbyte }) => {
      const broken = airbyte.store.connections.get(brokenConnection);
      airbyte.schema.onRefresh(brokenConnection, updatedAtDropped);
      // The server diffs the saved catalog against the source schema. Full refresh needs no cursor, so the saved
      // catalog has no breaking change any more. The mock does not compute this, so the override supplies it.
      // next() runs the normal mock save first.
      airbyte.override("webBackendUpdateConnection", ({ body, next, store }) => {
        next();
        return store.connections.update(body.connectionId, { schemaChange: "no_change" });
      });

      await test.step("the connection list shows the connection with an error and disabled", async () => {
        await connectionList.visit(page, airbyte.workspaceId);

        await connectionList.verifyHasErrorIcon(page, broken);
        await connectionList.verifyConnectionDisabled(page, broken);
      });

      await test.step("open the replication page from the error", async () => {
        await page.getByTestId(`link-replication-${broken.connectionId}`).click();

        await waitForReplicationPage(page);
      });

      await test.step("the breaking change banner shows, and the connection cannot be enabled", async () => {
        await schemaChange.verifySchemaChangeBanner(page, true);
        await expect.soft(connectionSwitch(page)).toBeDisabled();
      });

      await test.step("review the changes", async () => {
        await schemaChange.clickReviewChangesButton(page);

        await catalogDiffModal.verifyModalVisible(page);
        expect.soft(airbyte.calls.last("webBackendGetConnection")).toEqual({
          connectionId: broken.connectionId,
          withRefreshedCatalog: true,
        });
      });

      await test.step("the changes show that users lost the field updated_at", async () => {
        await catalogDiffModal.verifyNoRemovedStreams(page);
        await catalogDiffModal.verifyNoNewStreams(page);
        await catalogDiffModal.toggleStreamAccordion(page, "users");
        await catalogDiffModal.verifyRemovedFields(page, ["updated_at"]);
        await catalogDiffModal.verifyNoNewFields(page);
        await expect.soft(connectionSwitch(page)).toBeDisabled();
      });

      await test.step("close the changes, and the banner goes away", async () => {
        await catalogDiffModal.closeModal(page);

        await schemaChange.verifySchemaChangeBannerCleared(page);
      });

      await test.step("change users to Full refresh | Append and save", async () => {
        await streamsTable.filterByStreamName(page, "users");
        await streamsTable.selectSyncMode(page, "public", "users", SyncMode.full_refresh, DestinationSyncMode.append);
        await replicationForm.saveChanges(page, false);

        const update = airbyte.calls.last("webBackendUpdateConnection");
        expect.soft(update).toMatchObject({ connectionId: broken.connectionId, skipReset: true });
        expect.soft(findStream(update?.syncCatalog, "users")?.config).toMatchObject({
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        });
      });

      await test.step("the connection can be enabled again", async () => {
        await expect.soft(connectionSwitch(page)).toBeEnabled();
      });
    });
  });

  test.describe(() => {
    test.use({ airbyteScenario: breakingChangeScenario });

    test("clear a breaking schema change after the source is restored", async ({ page, airbyte }) => {
      const broken = airbyte.store.connections.get(brokenConnection);
      airbyte.schema.onRefresh(brokenConnection, updatedAtRestored);

      await test.step("the breaking change banner shows, and the connection cannot be enabled", async () => {
        await openReplicationPage(page, airbyte, brokenConnection);

        await schemaChange.verifySchemaChangeBanner(page, true);
        await expect.soft(connectionSwitch(page)).toBeDisabled();
      });

      await test.step("review the changes", async () => {
        await schemaChange.clickReviewChangesButton(page);

        await schemaChange.verifyNoDiffToast(page);
        expect.soft(airbyte.calls.last("webBackendGetConnection")).toEqual({
          connectionId: broken.connectionId,
          withRefreshedCatalog: true,
        });
      });

      await test.step("the banner goes away, and no changes show", async () => {
        await schemaChange.verifySchemaChangeBannerCleared(page);
        await catalogDiffModal.verifyModalNotVisible(page);
      });
    });
  });
});
