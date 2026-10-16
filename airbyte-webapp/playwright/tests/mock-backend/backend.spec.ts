/**
 * Self-tests of the mock backend (playwright/mock/backend.ts). They protect the rules that converted tests rely on.
 */
import type { SchemaRefreshAnswer, Scenario } from "../../mock";
import type { BrowserContext, Page } from "@playwright/test";
import type { FieldSpec } from "@src/core/api/types/AirbyteClient";

import { test, expect } from "@playwright/test";

import { catalogs, connection, defs, destination, errors, mapperValidation, scenario, source } from "../../mock";
import { createMockBackend } from "../../mock/backend";
import { MockResponse } from "../../mock/handlers/handler";

const pokeConnection = connection({
  source: source({ def: defs.pokeApiSource }),
  destination: destination({ def: defs.e2eTestingDestination }),
  catalog: catalogs.pokeApi({ selected: true }),
});

/** Installs a mock backend for a scenario, on the origin of baseURL, and opens a blank page to send requests from. */
async function installBackend(context: BrowserContext, page: Page, baseURL: string | undefined, input: Scenario) {
  const appOrigin = new URL(baseURL!).origin;
  const backend = createMockBackend(input, { appOrigin, apiOrigins: [appOrigin] });
  await backend.install(context);
  await page.goto("about:blank");
  return { backend, appOrigin };
}

/** Sends one JSON request from the page. Resolves with the status, the content type and the body text. */
function send(page: Page, url: string, method: string, body?: object) {
  return page.evaluate(
    async ({ url, method, body }) => {
      const response = await fetch(url, {
        method,
        headers: { "content-type": "application/json" },
        body: body && JSON.stringify(body),
        signal: AbortSignal.timeout(2000),
      });
      return {
        status: response.status,
        contentType: response.headers.get("content-type"),
        text: await response.text(),
      };
    },
    { url, method, body }
  );
}

/** Sends a mapper validation with one hashing mapper (ID m1) for the pokemon stream. The body replaces fields. */
function validateMappers(page: Page, appOrigin: string, body: object) {
  return send(page, `${appOrigin}/api/v1/web_backend/mappers/validate`, "POST", {
    streamDescriptor: { name: "pokemon" },
    mappers: [{ id: "m1", type: "hashing", mapperConfiguration: { targetField: "name" } }],
    ...body,
  });
}

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

  test("answers an operation with an override, and next() runs the default handler", async ({
    context,
    page,
    baseURL,
  }) => {
    const { backend, appOrigin } = await installBackend(
      context,
      page,
      baseURL,
      scenario({ connections: [pokeConnection] })
    );
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);
    backend.airbyte.override("webBackendGetConnection", ({ next }) => {
      const reply = next();
      return reply instanceof MockResponse ? reply : { ...reply, name: "Overridden" };
    });

    const read = await send(page, `${appOrigin}/api/v1/web_backend/connections/get`, "POST", { connectionId });

    expect(JSON.parse(read.text)).toMatchObject({ connectionId, name: "Overridden" });
    expect(backend.airbyte.calls.log().map(({ op, status }) => ({ op, status }))).toEqual([
      { op: "webBackendGetConnection", status: 200 },
    ]);
    expect(backend.problems()).toEqual([]);
  });

  test("sends an errors.problem reply as application/json", async ({ context, page, baseURL }) => {
    const { backend, appOrigin } = await installBackend(
      context,
      page,
      baseURL,
      scenario({ connections: [pokeConnection] })
    );
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);
    const conflict = {
      status: 409,
      type: "https://reference.airbyte.com/reference/errors#409-state-conflict",
      title: "state-conflict",
    };
    backend.airbyte.override("deleteConnection", () => errors.problem(conflict));

    const deletion = await send(page, `${appOrigin}/api/v1/connections/delete`, "POST", { connectionId });

    // The webapp parses an error body as JSON only when the content type is exactly application/json (apiCall.ts).
    expect(deletion).toEqual({ status: 409, contentType: "application/json", text: JSON.stringify(conflict) });
    expect(backend.airbyte.calls.log().map(({ op, status }) => ({ op, status }))).toEqual([
      { op: "deleteConnection", status: 409 },
    ]);
    expect(backend.problems()).toEqual([]);
  });

  test("type-checks the operation name and the reply of an override", () => {
    const backend = createMockBackend(scenario({}), {
      appOrigin: "https://localhost",
      apiOrigins: ["https://localhost"],
    });

    // `pnpm exec tsc` fails when a line below has no type error, because each line has a @ts-expect-error comment.
    // @ts-expect-error The generated route table has no operation with this name.
    backend.airbyte.override("noSuchOperation", () => undefined);
    // @ts-expect-error The reply of getWorkspace must be a WorkspaceRead.
    backend.airbyte.override("getWorkspace", () => ({ notAWorkspace: true }));
  });

  test("changes the answer of an operation during a test when the test sets a new override", async ({
    context,
    page,
    baseURL,
  }) => {
    const { backend, appOrigin } = await installBackend(
      context,
      page,
      baseURL,
      scenario({ connections: [pokeConnection] })
    );
    const { workspaceId } = backend.airbyte;
    const getWorkspace = () => send(page, `${appOrigin}/api/v1/workspaces/get`, "POST", { workspaceId });

    const before = await getWorkspace();
    backend.airbyte.override("getWorkspace", () => errors.notFound());
    const after = await getWorkspace();

    expect(before.status).toBe(200);
    expect(after).toMatchObject({
      status: 404,
      text: JSON.stringify({ errorId: "00000000-0000-4000-8000-000000000404" }),
    });
    expect(backend.problems()).toEqual([]);
  });

  test("fails the test when an override calls next() for an operation without a default handler", async ({
    context,
    page,
    baseURL,
  }) => {
    const { backend, appOrigin } = await installBackend(
      context,
      page,
      baseURL,
      scenario({ connections: [pokeConnection] })
    );
    backend.airbyte.override("syncConnection", ({ next }) => next());

    const sync = await send(page, `${appOrigin}/api/v1/connections/sync`, "POST", { connectionId: "any" });

    expect(sync.status).toBe(500);
    expect(backend.problems()).toEqual([
      "Handler error: POST /api/v1/connections/sync (syncConnection): The override called next(), but the mock has no default handler for syncConnection",
    ]);
  });

  test("answers every Airbyte API call with the failAll response, but still answers external hosts", async ({
    context,
    page,
    baseURL,
  }) => {
    const { backend, appOrigin } = await installBackend(
      context,
      page,
      baseURL,
      scenario({ connections: [pokeConnection] })
    );
    backend.airbyte.override("getWorkspace", () => backend.airbyte.store.identity.workspace);
    backend.airbyte.failAll(errors.badGateway());

    const config = await send(page, `${appOrigin}/api/v1/web_backend/config`, "GET");
    const workspace = await send(page, `${appOrigin}/api/v1/workspaces/get`, "POST", { workspaceId: "any" });
    const sync = await send(page, `${appOrigin}/api/v1/connections/sync`, "POST", { connectionId: "any" });
    const securityCheck = await send(page, "https://oss.airbyte.com/security-check", "POST", { host: appOrigin });

    // An operation without a default handler and an overridden operation fail the same way.
    for (const response of [config, workspace, sync]) {
      expect(response).toEqual({ status: 502, contentType: "text/plain", text: "502 Bad Gateway" });
    }
    expect(securityCheck.status).toBe(200);
    expect(backend.airbyte.calls.count("getWebappConfig")).toBe(1);
    expect(backend.problems()).toEqual([]);
  });

  test("answers the security check of oss.airbyte.com from the scenario", async ({ browser, baseURL }) => {
    const answers = ["closed", "open", "default_auth", "fail", "hang"] as const;
    const results = [];
    for (const answer of answers) {
      const context = await browser.newContext({ storageState: { cookies: [], origins: [] } });
      const page = await context.newPage();
      const input = scenario({ external: { securityCheck: answer } });
      const { backend, appOrigin } = await installBackend(context, page, baseURL, input);
      const host = { host: appOrigin };

      // "hang" never answers. send() stops waiting after 2 seconds and throws, so the result is "no answer".
      results.push(await send(page, "https://oss.airbyte.com/security-check", "POST", host).catch(() => "no answer"));
      expect(backend.problems()).toEqual([]);
      await context.close();
    }

    expect(results).toEqual([
      { status: 200, contentType: "application/json", text: JSON.stringify({ status: "closed" }) },
      { status: 200, contentType: "application/json", text: JSON.stringify({ status: "open" }) },
      { status: 200, contentType: "application/json", text: JSON.stringify({ status: "default_auth" }) },
      { status: 500, contentType: "text/plain", text: "Internal Server Error" },
      "no answer",
    ]);
  });

  test("answers a schema refresh with the onRefresh answer, and fails a refresh that has no answer", async ({
    context,
    page,
    baseURL,
  }) => {
    const input = scenario({ connections: [pokeConnection], experiments: { asyncSchemaDiscovery: false } });
    const { backend, appOrigin } = await installBackend(context, page, baseURL, input);
    const { connectionId, syncCatalog } = backend.airbyte.store.connections.get(pokeConnection);
    const getConnection = (withRefreshedCatalog: boolean) =>
      send(page, `${appOrigin}/api/v1/web_backend/connections/get`, "POST", { connectionId, withRefreshedCatalog });
    const refreshedCatalog = catalogs.pokeApi({ selected: true, withoutFields: { pokemon: ["base_experience"] } });
    const diff = { transforms: [] };

    const withoutAnswer = await getConnection(true);
    backend.airbyte.schema.onRefresh(pokeConnection, { catalog: refreshedCatalog, diff, schemaChange: "breaking" });
    const refreshed = await getConnection(true);
    const stored = await getConnection(false);

    expect(withoutAnswer.status).toBe(500);
    expect(JSON.parse(refreshed.text)).toMatchObject({
      syncCatalog: refreshedCatalog,
      catalogDiff: diff,
      schemaChange: "breaking",
    });
    // The refresh does not change the stored connection.
    expect(JSON.parse(stored.text)).toMatchObject({ syncCatalog, schemaChange: "no_change" });
    expect(backend.problems()).toEqual([
      `Handler error: POST /api/v1/web_backend/connections/get (webBackendGetConnection): The webapp refreshed the schema of connection ${connectionId}, but the test set no answer. Call airbyte.schema.onRefresh() before the refresh.`,
    ]);
  });

  test("onRefresh fails when the scenario does not pin the synchronous schema refresh", () => {
    const backend = createMockBackend(scenario({ connections: [pokeConnection] }), {
      appOrigin: "https://localhost",
      apiOrigins: ["https://localhost"],
    });
    const answer: SchemaRefreshAnswer = {
      catalog: catalogs.pokeApi(),
      diff: { transforms: [] },
      schemaChange: "no_change",
    };

    expect(() => backend.airbyte.schema.onRefresh(pokeConnection, answer)).toThrow(
      "Set experiments: { asyncSchemaDiscovery: false } in the scenario."
    );
  });

  test("answers a mapper validation with every mapper valid and no fields", async ({ context, page, baseURL }) => {
    const { backend, appOrigin } = await installBackend(
      context,
      page,
      baseURL,
      scenario({ connections: [pokeConnection] })
    );
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);

    const response = await validateMappers(page, appOrigin, { connectionId });

    expect(JSON.parse(response.text)).toEqual({
      initialFields: [],
      outputFields: [],
      mappers: [{ id: "m1", inputFields: [], outputFields: [] }],
    });
  });

  test("answers a mapper validation with the fields that allValid gives", async ({ context, page, baseURL }) => {
    const { backend, appOrigin } = await installBackend(
      context,
      page,
      baseURL,
      scenario({ connections: [pokeConnection] })
    );
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);
    const fields: FieldSpec[] = [
      { name: "name", type: "STRING", isSelectedPrimaryKey: false, isSelectedCursor: false },
    ];
    backend.airbyte.override("webBackendValidateMappers", mapperValidation.allValid(fields));

    const response = await validateMappers(page, appOrigin, { connectionId });

    expect(JSON.parse(response.text)).toEqual({
      initialFields: fields,
      outputFields: fields,
      mappers: [{ id: "m1", inputFields: fields, outputFields: fields }],
    });
  });

  test("answers 404 to a mapper validation for an unknown connection", async ({ context, page, baseURL }) => {
    const { appOrigin } = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));

    const response = await validateMappers(page, appOrigin, { connectionId: "c0000000-0000-4000-8000-000000000404" });

    expect(response.status).toBe(404);
  });

  test("fails the test on a mapper validation for a stream the connection does not have", async ({
    context,
    page,
    baseURL,
  }) => {
    const { backend, appOrigin } = await installBackend(
      context,
      page,
      baseURL,
      scenario({ connections: [pokeConnection] })
    );
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);

    const response = await validateMappers(page, appOrigin, {
      connectionId,
      streamDescriptor: { namespace: "public", name: "users" },
    });

    expect(response.status).toBe(500);
    expect(backend.problems()).toEqual([
      `Handler error: POST /api/v1/web_backend/mappers/validate (webBackendValidateMappers): Connection ${connectionId} has no stream public.users`,
    ]);
  });

  test("starts an instance before setup, and the setup form completes it", async ({ context, page, baseURL }) => {
    const input = scenario({ instance: { initialSetupComplete: false } });
    const { backend, appOrigin } = await installBackend(context, page, baseURL, input);
    const { userId } = backend.airbyte.store.identity.user;
    const getInstance = () => send(page, `${appOrigin}/api/v1/instance_configuration`, "GET");
    const setupForm = {
      email: "admin@example.com",
      organizationName: "ACME Corp",
      anonymousDataCollection: true,
      initialSetupComplete: true,
      displaySetupWizard: false,
    };

    const before = JSON.parse((await getInstance()).text);
    const setup = await send(page, `${appOrigin}/api/v1/instance_configuration/setup`, "POST", setupForm);
    const after = JSON.parse((await getInstance()).text);
    const user = JSON.parse((await send(page, `${appOrigin}/api/v1/users/get`, "POST", { userId })).text);
    const workspaces = JSON.parse(
      (await send(page, `${appOrigin}/api/v1/workspaces/list_by_user_id`, "POST", { userId })).text
    );

    expect(before).toMatchObject({ initialSetupComplete: false, defaultOrganizationEmail: "test@example.com" });
    expect(JSON.parse(setup.text)).toEqual(after);
    expect(after).toMatchObject({ initialSetupComplete: true, defaultOrganizationEmail: "admin@example.com" });
    expect(user).toMatchObject({ userId, email: "admin@example.com" });
    expect(workspaces.workspaces).toEqual([
      expect.objectContaining({
        email: "admin@example.com",
        initialSetupComplete: true,
        anonymousDataCollection: true,
      }),
    ]);
    expect(backend.problems()).toEqual([]);
  });
});
