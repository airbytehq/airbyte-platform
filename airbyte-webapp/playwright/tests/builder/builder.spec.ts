/**
 * Connector Builder tests: build a stream, test it, and come back to a saved project.
 *
 * Every test runs against the mock backend in playwright/mock. Each test starts from the backend state in its own
 * scenario, so no test depends on what an earlier test did.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it.
 *
 * The server runs a test read with the connector code, so the mock never computes its result. Each scenario declares
 * the result of the test reads (`outcomes.streamRead`). The tests check the read request that the builder sends, and
 * how the builder shows the result.
 */
import type { MockAirbyte } from "../../mock";
import type { Page } from "@playwright/test";

import {
  assertAutoImportSchemaMatches,
  assertHasNumberOfSlices,
  assertNumberOfPages,
  assertNumberOfPagesInEachSlice,
  assertSchemaMismatch,
  assertTestPanelContains,
  assertTestReadLimitReached,
  assertUrlPath,
} from "./assertions";
import {
  AUTO_IMPORTED_SCHEMA,
  BUILDER_CONNECTOR_NAME,
  SCHEMA_WITH_MISMATCH,
  configureInvalidAuth,
  configureListPartitionRouter,
  configurePagination,
  configureSchemaLoader,
  configureStream,
  configureValidAuth,
  initializeBuilderConnector,
  navigateToDetectedSchema,
  navigateToStreamSchema,
  testStream,
  updateUrlPath,
  waitForDraftToSave,
} from "./helpers";
import { expect, scenario, streamRead, test } from "../../mock";

/** Starts a connector with an Items stream at https://api.example.com/items/ and bearer token auth. */
async function startItemsConnector(page: Page, airbyte: MockAirbyte) {
  await initializeBuilderConnector(page, airbyte.workspaceId);
  await configureStream(page);
  await configureValidAuth(page);
}

/** The first stream of a connector manifest, such as the manifest of a test read request or of a saved draft. */
function firstStream(manifest: unknown) {
  return (manifest as { streams?: unknown[] } | undefined)?.streams?.[0];
}

/** The first stream of the manifest in the latest test read request. */
function streamOfLatestRead(airbyte: MockAirbyte) {
  return firstStream(airbyte.calls.last("readConnectorBuilderProjectStream")?.manifest);
}

/** The schema that the server detects from the records abc and def. */
const nameSchema = {
  $schema: "http://json-schema.org/schema#",
  properties: { name: { type: ["string", "null"] } },
  type: "object",
};

/** The schema that the server detects when the records abc and def also have the field id. */
const nameAndIdSchema = {
  $schema: "http://json-schema.org/schema#",
  properties: { id: { type: ["integer", "null"] }, name: { type: ["string", "null"] } },
  type: "object",
};

/**
 * The stream schema that the builder shows after it imports nameAndIdSchema from a test read. The builder adds
 * "additionalProperties": true.
 */
const AUTO_IMPORTED_NAME_AND_ID_SCHEMA = `{
  "$schema": "http://json-schema.org/schema#",
  "additionalProperties": true,
  "properties": {
    "id": {
      "type": [
        "integer",
        "null"
      ]
    },
    "name": {
      "type": [
        "string",
        "null"
      ]
    }
  },
  "type": "object"
}`;

test.describe(() => {
  // Every test read returns the records abc and def on one page. The test changes the answer before its last read.
  test.use({
    airbyteScenario: scenario({
      outcomes: {
        streamRead: streamRead.pages({ pages: [[{ name: "abc" }, { name: "def" }]], inferredSchema: nameSchema }),
      },
    }),
  });

  test("test a stream and compare its detected schema with the declared schema", async ({ page, airbyte }) => {
    await test.step("start a connector with an Items stream and bearer token auth", async () => {
      await startItemsConnector(page, airbyte);
    });

    await test.step("turn off the automatic schema import, and declare a schema without the field name", async () => {
      await navigateToStreamSchema(page);
      await configureSchemaLoader(page, SCHEMA_WITH_MISMATCH);
    });

    await test.step("test the stream", async () => {
      await testStream(page);
    });

    await test.step("the test panel shows the records of the read", async () => {
      await assertTestPanelContains(page, '"name": "abc"');
      await assertTestPanelContains(page, '"name": "def"');
    });

    await test.step("the read request has the Items stream with bearer token auth and the declared schema", async () => {
      expect.soft(airbyte.calls.last("readConnectorBuilderProjectStream")).toMatchObject({ streamName: "Items" });
      expect.soft(streamOfLatestRead(airbyte)).toMatchObject({
        retriever: {
          requester: {
            url: "https://api.example.com/items/",
            authenticator: { type: "BearerAuthenticator" },
          },
          record_selector: { extractor: { field_path: ["items"] } },
        },
        schema_loader: { schema: JSON.parse(SCHEMA_WITH_MISMATCH) },
      });
    });

    await test.step("the saved testing values have the bearer token", async () => {
      expect.soft(airbyte.calls.last("updateConnectorBuilderProjectTestingValues")).toMatchObject({
        testingValues: { api_key: "theauthkey" },
      });
    });

    await test.step("the detected schema tab shows the conflict with the declared schema", async () => {
      await navigateToDetectedSchema(page);
      await assertSchemaMismatch(page);
    });

    await test.step("turn on the automatic schema import", async () => {
      await navigateToStreamSchema(page);
      await page.locator('label:has-text("Automatically import detected schema")').click();
    });

    await test.step("the stream schema is the detected schema, and allows other fields", async () => {
      await assertAutoImportSchemaMatches(page, AUTO_IMPORTED_SCHEMA);
    });

    await test.step("test the stream again after the API adds the field id to its records", async () => {
      // From now on, every test read returns records with the fields name and id.
      airbyte.override("readConnectorBuilderProjectStream", () =>
        streamRead.pages({
          pages: [
            [
              { name: "abc", id: 1 },
              { name: "def", id: 2 },
            ],
          ],
          inferredSchema: nameAndIdSchema,
        })
      );
      await testStream(page);
    });

    await test.step("the stream schema is the new detected schema with the field id", async () => {
      await assertAutoImportSchemaMatches(page, AUTO_IMPORTED_NAME_AND_ID_SCHEMA);
    });
  });
});

test.describe(() => {
  // The first test read fails with 404, as the endpoint does not exist. The test sets the answer to the next read.
  test.use({
    airbyteScenario: scenario({
      outcomes: { streamRead: streamRead.failedRequest({ status: 404, body: { error: "Not found" } }) },
    }),
  });

  test("find out why a test read fails", async ({ page, airbyte }) => {
    await test.step("start a connector with an Items stream and bearer token auth", async () => {
      await startItemsConnector(page, airbyte);
    });

    await test.step("change the URL to an endpoint that does not exist, and test the stream", async () => {
      await updateUrlPath(page, "nonexistent-endpoint/");
      await testStream(page);
    });

    await test.step("the read request has the URL of the endpoint that does not exist", async () => {
      expect.soft(streamOfLatestRead(airbyte)).toMatchObject({
        retriever: { requester: { url: "https://api.example.com/nonexistent-endpoint/" } },
      });
    });

    await test.step("the test panel shows the 404 response", async () => {
      await assertTestPanelContains(page, '"status": 404');
    });

    await test.step("turn off authentication, and test the stream", async () => {
      // From now on, every test read fails with 403 Bad credentials.
      airbyte.override("readConnectorBuilderProjectStream", () =>
        streamRead.failedRequest({ status: 403, body: { error: "Bad credentials" } })
      );
      await configureInvalidAuth(page);
      await testStream(page);
    });

    await test.step("the read request has no authentication", async () => {
      expect.soft(streamOfLatestRead(airbyte)).toMatchObject({
        retriever: { requester: { authenticator: { type: "NoAuth" } } },
      });
    });

    await test.step("the test panel shows the response of the API: Bad credentials", async () => {
      await assertTestPanelContains(page, '"Bad credentials"');
    });
  });
});

test.describe(() => {
  // Every test read returns 5 pages of 2 records, and reports that it stopped at the page limit.
  test.use({
    airbyteScenario: scenario({
      outcomes: {
        streamRead: streamRead.pages({
          pages: [
            [{ name: "abc" }, { name: "def" }],
            [{ name: "xxx" }, { name: "yyy" }],
            [{ name: "page 3, record 1" }, { name: "page 3, record 2" }],
            [{ name: "page 4, record 1" }, { name: "page 4, record 2" }],
            [{ name: "page 5, record 1" }, { name: "page 5, record 2" }],
          ],
          limitReached: true,
        }),
      },
    }),
  });

  test("test a paginated stream up to the page limit of a test read", async ({ page, airbyte }) => {
    await test.step("start a connector with an Items stream and bearer token auth", async () => {
      await startItemsConnector(page, airbyte);
    });

    await test.step("add offset pagination with 2 records per page, and test the stream", async () => {
      await configurePagination(page);
      await testStream(page);
    });

    await test.step("the test read asks for 5 pages at most, with the offset pagination", async () => {
      expect
        .soft(airbyte.calls.last("readConnectorBuilderProjectStream"))
        .toMatchObject({ streamName: "Items", recordLimit: 1000, pageLimit: 5, sliceLimit: 5 });
      expect.soft(streamOfLatestRead(airbyte)).toMatchObject({
        retriever: {
          paginator: {
            pagination_strategy: { type: "OffsetIncrement", page_size: 2 },
            page_size_option: { inject_into: "header", field_name: "limit" },
            page_token_option: { inject_into: "header", field_name: "offset" },
          },
        },
      });
    });

    await test.step("the test panel shows the 5 pages of the read, and that the read stopped at a limit", async () => {
      await assertNumberOfPages(page, 5);
      await assertTestReadLimitReached(page);
      await assertTestPanelContains(page, '"name": "abc"');
      await assertTestPanelContains(page, '"name": "def"');
    });

    await test.step("open page 2", async () => {
      await page.locator('[data-testid="test-pages"]').locator('a[aria-label="Page 2"]').click();
    });

    await test.step("the test panel shows the records of page 2", async () => {
      await assertTestPanelContains(page, '"name": "xxx"');
      await assertTestPanelContains(page, '"name": "yyy"');
    });
  });
});

test.describe(() => {
  // Every test read returns 5 partitions with 5 pages each, and reports that it stopped at a limit.
  test.use({
    airbyteScenario: scenario({
      outcomes: {
        streamRead: streamRead.grid({ slices: 5, pagesPerSlice: 5, recordsPerPage: 2, limitReached: true }),
      },
    }),
  });

  test("test a partitioned stream up to the partition and page limits of a test read", async ({ page, airbyte }) => {
    await test.step("start a connector with an Items stream and bearer token auth", async () => {
      await startItemsConnector(page, airbyte);
    });

    await test.step("partition the requests by a list of 10 item IDs, and add offset pagination", async () => {
      await updateUrlPath(page, "items/{{ stream_partition.item_id }}");
      await configureListPartitionRouter(page, 10);
      await configurePagination(page);
    });

    await test.step("test the stream", async () => {
      await testStream(page);
    });

    await test.step("the read request has the 10 item IDs, and asks for 5 partitions and 5 pages at most", async () => {
      expect
        .soft(airbyte.calls.last("readConnectorBuilderProjectStream"))
        .toMatchObject({ streamName: "Items", recordLimit: 1000, pageLimit: 5, sliceLimit: 5 });
      expect.soft(streamOfLatestRead(airbyte)).toMatchObject({
        retriever: {
          requester: { url: "https://api.example.com/items/{{ stream_partition.item_id }}" },
          partition_router: {
            type: "ListPartitionRouter",
            cursor_field: "item_id",
            values: ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
          },
          paginator: { pagination_strategy: { type: "OffsetIncrement", page_size: 2 } },
        },
      });
    });

    await test.step("the test panel shows the 5 partitions of the read, and that the read stopped at a limit", async () => {
      await assertTestReadLimitReached(page);
      await assertHasNumberOfSlices(page, 5);
    });

    await test.step("each partition shows its 5 pages", async () => {
      await assertNumberOfPagesInEachSlice(page, 5, 5);
    });
  });
});

test.describe(() => {
  // A workspace without Connector Builder projects. No test reads.
  test.use({ airbyteScenario: scenario({}) });

  test("return to a saved connector after leaving the builder", async ({ page, airbyte }) => {
    await test.step("start a connector with an Items stream, partitioned by a list of 10 item IDs", async () => {
      await startItemsConnector(page, airbyte);
      await updateUrlPath(page, "items/{{ stream_slice.item_id }}");
      await configureListPartitionRouter(page, 10);
    });

    await test.step("the builder saves the draft", async () => {
      await waitForDraftToSave(page);
      const draft = airbyte.calls.last("updateConnectorBuilderProject")?.builderProject.draftManifest;
      expect.soft(firstStream(draft)).toMatchObject({
        retriever: { requester: { url: "https://api.example.com/items/{{ stream_slice.item_id }}" } },
      });
    });

    await test.step("leave the builder, and open the connector from the list of projects", async () => {
      await page.goto(`/workspaces/${airbyte.workspaceId}/connector-builder`);
      await page.locator(`[data-testid="edit-project-button-${BUILDER_CONNECTOR_NAME}"]`).click();
      await expect(page.locator('input[data-field-path="manifest.streams.0.name"]')).toBeVisible();
    });

    await test.step("the stream keeps its URL", async () => {
      await assertUrlPath(page, "items/{{ stream_slice.item_id }}");
    });
  });
});
