/**
 * Self-tests of the mock backend (playwright/mock/backend.ts). They protect the rules that converted tests rely on.
 */
import { test, expect } from "@playwright/test";

import { catalogs, connection, defs, destination, scenario, source } from "../../mock";
import { createMockBackend } from "../../mock/backend";

const pokeConnection = connection({
  source: source({ def: defs.pokeApiSource }),
  destination: destination({ def: defs.e2eTestingDestination }),
  catalog: catalogs.pokeApi({ selected: true }),
});

test.describe("Mock backend", () => {
  // These tests use no deployed backend, so they do not use the signed-in state of the setup project.
  test.use({ storageState: { cookies: [], origins: [] } });

  test("fails the test on a request that no handler answers, naming method, path and operationId", async ({
    context,
    page,
    baseURL,
  }) => {
    const appOrigin = new URL(baseURL!).origin;
    const backend = createMockBackend(scenario({ connections: [pokeConnection] }), {
      appOrigin,
      apiOrigins: [appOrigin],
    });
    await backend.install(context);

    await page.goto("about:blank");
    await page.evaluate(async (origin) => {
      const send = (path: string) => fetch(`${origin}${path}`, { method: "POST", body: "{}" }).catch(() => undefined);
      await send("/api/v1/connections/sync");
      await send("/api/v1/no_such_endpoint");
      await fetch("https://external.example.com/script.js").catch(() => undefined);
    }, appOrigin);

    expect(backend.problems()).toEqual([
      "Unexpected request: POST /api/v1/connections/sync (syncConnection)",
      "Unexpected request: POST /api/v1/no_such_endpoint (unknown operation)",
      "Unexpected request: GET https://external.example.com/script.js (external host)",
    ]);
    expect(() => backend.verify()).toThrow("POST /api/v1/connections/sync (syncConnection)");
  });

  test("fails the test on a body that is not JSON, and on a connection list request it cannot answer", async ({
    context,
    page,
    baseURL,
  }) => {
    const appOrigin = new URL(baseURL!).origin;
    const backend = createMockBackend(scenario({ connections: [pokeConnection] }), {
      appOrigin,
      apiOrigins: [appOrigin],
    });
    await backend.install(context);
    const { workspaceId } = backend.airbyte;
    const unusedFilters = { searchTerm: "", statuses: undefined, tagIds: [], onDemandEnabled: null };

    await page.goto("about:blank");
    const statuses = await page.evaluate(
      async ({ origin, workspaceId, unusedFilters }) => {
        const send = (path: string, body: string) =>
          fetch(`${origin}${path}`, { method: "POST", body }).then((response) => response.status);
        const list = (body: object) =>
          send("/api/v1/web_backend/connections/list", JSON.stringify({ workspaceId, ...body }));
        return [
          await send("/api/v1/web_backend/connections/get", "not json"),
          await list({ filters: unusedFilters }),
          await list({ cursor: "last-connection-id" }),
          await list({ filters: { ...unusedFilters, searchTerm: "poke" } }),
        ];
      },
      { origin: appOrigin, workspaceId, unusedFilters }
    );

    // A list request whose filters are all empty is answered. The other requests are handler errors.
    expect(statuses).toEqual([500, 200, 500, 500]);
    expect(backend.problems()).toEqual([
      "Handler error: POST /api/v1/web_backend/connections/get (webBackendGetConnection): POST data is not a valid JSON object: not json",
      "Handler error: POST /api/v1/web_backend/connections/list (webBackendListConnectionsForWorkspace): The mock does not support paging or filters on the connection list",
      "Handler error: POST /api/v1/web_backend/connections/list (webBackendListConnectionsForWorkspace): The mock does not support paging or filters on the connection list",
    ]);
  });
});
