/**
 * Self-tests of the Connector Builder projects (playwright/mock/builderProjects.ts) and test reads
 * (playwright/mock/builders/streamRead.ts). The browser page stands in for the webapp and sends the same requests.
 */
import { test, expect } from "@playwright/test";

import { installBackend, post } from "./selfTest";
import { scenario, streamRead } from "../../mock";

const manifest = { version: "6.0.0", type: "DeclarativeSource", streams: [{ name: "Items" }] };

test.describe("Mock backend Connector Builder", () => {
  // These tests use no deployed backend, so they do not use the signed-in state of the setup project.
  test.use({ storageState: { cookies: [], origins: [] } });

  test("saves the draft and the testing values of a project, and returns both on a read", async ({
    context,
    page,
    baseURL,
  }) => {
    const backend = await installBackend(context, page, baseURL, scenario({}));
    const { workspaceId } = backend.airbyte;

    const created = await post(page, "connector_builder_projects/create", {
      workspaceId,
      builderProject: { name: "Draft" },
    });
    const { builderProjectId } = created.body;
    const empty = await post(page, "connector_builder_projects/get_with_manifest", { workspaceId, builderProjectId });
    await post(page, "connector_builder_projects/update", {
      workspaceId,
      builderProjectId,
      builderProject: { name: "Items API", draftManifest: manifest },
    });
    await post(page, "connector_builder_projects/update_testing_values", {
      workspaceId,
      builderProjectId,
      spec: {},
      testingValues: { api_key: "secret" },
    });
    const saved = await post(page, "connector_builder_projects/get_with_manifest", { workspaceId, builderProjectId });
    const list = await post(page, "connector_builder_projects/list", { workspaceId });

    expect(builderProjectId).toBe("bb000000-0000-4000-8000-000000000001");
    expect(empty.body).toMatchObject({ builderProject: { name: "Draft", hasDraft: false } });
    expect(saved.body).toMatchObject({
      builderProject: { builderProjectId, name: "Items API", hasDraft: true },
      declarativeManifest: { manifest, isDraft: true },
      testingValues: { api_key: "secret" },
    });
    expect(list.body.projects).toEqual([expect.objectContaining({ builderProjectId, name: "Items API" })]);
    expect(backend.problems()).toEqual([]);
  });

  test("answers a test read with the result the scenario declares, and echoes the manifest to resolve", async ({
    context,
    page,
    baseURL,
  }) => {
    const result = streamRead.grid({ slices: 2, pagesPerSlice: 3, recordsPerPage: 1, limitReached: true });
    const backend = await installBackend(context, page, baseURL, scenario({ outcomes: { streamRead: result } }));
    const { workspaceId } = backend.airbyte;
    const { builderProjectId } = (
      await post(page, "connector_builder_projects/create", { workspaceId, builderProject: { name: "A" } })
    ).body;

    const read = await post(page, "connector_builder_projects/read_stream", {
      workspaceId,
      builderProjectId,
      manifest,
      streamName: "A",
    });
    const resolved = await post(page, "connector_builder_projects/resolve", {
      workspaceId,
      builderProjectId,
      manifest,
    });

    expect(read.body).toEqual(result);
    expect(read.body.slices.map(({ pages }: { pages: unknown[] }) => pages.length)).toEqual([3, 3]);
    expect(read.body.slices[1].pages[2].records).toEqual([{ id: "slice-2-page-3-record-1" }]);
    expect(resolved.body).toEqual({ manifest });
    expect(backend.problems()).toEqual([]);
  });

  test("fails the test on a test read when the scenario declares no result", async ({ context, page, baseURL }) => {
    const backend = await installBackend(context, page, baseURL, scenario({}));
    const { workspaceId } = backend.airbyte;
    const { builderProjectId } = (
      await post(page, "connector_builder_projects/create", { workspaceId, builderProject: { name: "A" } })
    ).body;

    const read = await post(page, "connector_builder_projects/read_stream", {
      workspaceId,
      builderProjectId,
      manifest,
      streamName: "A",
    });

    expect(read.status).toBe(500);
    expect(backend.problems()).toEqual([
      "Handler error: POST /api/v1/connector_builder_projects/read_stream (readConnectorBuilderProjectStream): " +
        "The scenario declares no test read result. Set outcomes.streamRead, such as streamRead.grid()",
    ]);
  });
});
