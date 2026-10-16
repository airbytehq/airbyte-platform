/**
 * Self-tests of the mock backend (playwright/mock/backend.ts). They protect the rules that converted tests rely on.
 */
import type { SchemaRefreshAnswer } from "../../mock";
import type { Page } from "@playwright/test";
import type { FieldSpec } from "@src/core/api/types/AirbyteClient";

import { test, expect } from "@playwright/test";

import { installBackend, post } from "./selfTest";
import {
  catalogs,
  checkOutput,
  connection,
  defs,
  destination,
  errors,
  mapperValidation,
  scenario,
  source,
} from "../../mock";
import { createMockBackend } from "../../mock/backend";
import { MockResponse } from "../../mock/handlers/handler";

const pokeConnection = connection({
  source: source({ def: defs.pokeApiSource }),
  destination: destination({ def: defs.e2eTestingDestination }),
  catalog: catalogs.pokeApi({ selected: true }),
});

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
function validateMappers(page: Page, body: object) {
  return send(page, "/api/v1/web_backend/mappers/validate", "POST", {
    streamDescriptor: { name: "pokemon" },
    mappers: [{ id: "m1", type: "hashing", mapperConfiguration: { targetField: "name" } }],
    ...body,
  });
}

/**
 * Saves a PokeAPI source and an End-to-End Testing destination as drafts, as the create pages do before they check
 * the connection. Returns their IDs.
 */
async function createDrafts(page: Page, workspaceId: string) {
  const source = await post(page, "sources/create", {
    workspaceId,
    sourceDefinitionId: defs.pokeApiSource.input.definition.sourceDefinitionId,
    name: "PokeAPI draft",
    connectionConfiguration: { pokemon_name: "ditto" },
    createAsDraft: true,
  });
  const destination = await post(page, "destinations/create", {
    workspaceId,
    destinationDefinitionId: defs.e2eTestingDestination.input.definition.destinationDefinitionId,
    name: "E2E Testing draft",
    connectionConfiguration: {},
    createAsDraft: true,
  });
  return { sourceId: source.body.sourceId, destinationId: destination.body.destinationId };
}

/**
 * Checks a source or a destination as the webapp does: it starts the check, reads its status once, then reads its
 * output. The mock ends the check when the webapp reads its status. Returns the output.
 */
async function checkActor(page: Page, checkId: string, actorId: string) {
  await post(page, "commands/run/check", { id: checkId, actor_id: actorId });
  await post(page, "commands/status", { id: checkId });
  const output = await post(page, "commands/output/check", { id: checkId });
  return output.body;
}

test.describe("Mock backend", () => {
  test("fails the test on a request that no handler answers, naming method, path and operationId", async ({
    context,
    page,
    baseURL,
  }) => {
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));

    await page.evaluate(async () => {
      const send = (path: string) => fetch(path, { method: "POST", body: "{}" }).catch(() => undefined);
      await send("/api/v1/connections/reset");
      await send("/api/v1/no_such_endpoint");
      await fetch("https://external.example.com/script.js").catch(() => undefined);
    });

    expect(backend.problems()).toEqual([
      "Unexpected request: POST /api/v1/connections/reset (resetConnection)",
      "Unexpected request: POST /api/v1/no_such_endpoint (unknown operation)",
      "Unexpected request: GET https://external.example.com/script.js (external host)",
    ]);
    expect(() => backend.verify()).toThrow("POST /api/v1/connections/reset (resetConnection)");
  });

  test("fails the test on a body that is not JSON, and on a connection list request it cannot answer", async ({
    context,
    page,
    baseURL,
  }) => {
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    const { workspaceId } = backend.airbyte;
    const unusedFilters = { searchTerm: "", statuses: undefined, tagIds: [], onDemandEnabled: null };

    const statuses = await page.evaluate(
      async ({ workspaceId, unusedFilters }) => {
        const send = (path: string, body: string) =>
          fetch(path, { method: "POST", body }).then((response) => response.status);
        const list = (body: object) =>
          send("/api/v1/web_backend/connections/list", JSON.stringify({ workspaceId, ...body }));
        return [
          await send("/api/v1/web_backend/connections/get", "not json"),
          await list({ filters: unusedFilters }),
          await list({ cursor: "last-connection-id" }),
          await list({ filters: { ...unusedFilters, searchTerm: "poke" } }),
        ];
      },
      { workspaceId, unusedFilters }
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
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);
    backend.airbyte.override("webBackendGetConnection", ({ next }) => {
      const reply = next();
      return reply instanceof MockResponse ? reply : { ...reply, name: "Overridden" };
    });

    const read = await send(page, "/api/v1/web_backend/connections/get", "POST", { connectionId });

    expect(JSON.parse(read.text)).toMatchObject({ connectionId, name: "Overridden" });
    expect(backend.airbyte.calls.log().map(({ op, status }) => ({ op, status }))).toEqual([
      { op: "webBackendGetConnection", status: 200 },
    ]);
    expect(backend.problems()).toEqual([]);
  });

  test("sends an errors.problem reply as application/json", async ({ context, page, baseURL }) => {
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);
    const conflict = {
      status: 409,
      type: "https://reference.airbyte.com/reference/errors#409-state-conflict",
      title: "state-conflict",
    };
    backend.airbyte.override("deleteConnection", () => errors.problem(conflict));

    const deletion = await send(page, "/api/v1/connections/delete", "POST", { connectionId });

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
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    const { workspaceId } = backend.airbyte;
    const getWorkspace = () => send(page, "/api/v1/workspaces/get", "POST", { workspaceId });

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
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    backend.airbyte.override("resetConnection", ({ next }) => next());

    const reset = await send(page, "/api/v1/connections/reset", "POST", { connectionId: "any" });

    expect(reset.status).toBe(500);
    expect(backend.problems()).toEqual([
      "Handler error: POST /api/v1/connections/reset (resetConnection): The override called next(), but the mock has no default handler for resetConnection",
    ]);
  });

  test("answers every Airbyte API call with the failAll response, but still answers external hosts", async ({
    context,
    page,
    baseURL,
  }) => {
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    backend.airbyte.override("getWorkspace", () => backend.airbyte.store.identity.workspace);
    backend.airbyte.failAll(errors.badGateway());

    const config = await send(page, "/api/v1/web_backend/config", "GET");
    const workspace = await send(page, "/api/v1/workspaces/get", "POST", { workspaceId: "any" });
    const reset = await send(page, "/api/v1/connections/reset", "POST", { connectionId: "any" });
    const securityCheck = await send(page, "https://oss.airbyte.com/security-check", "POST", {
      host: new URL(baseURL!).origin,
    });

    // An operation without a default handler and an overridden operation fail the same way.
    for (const response of [config, workspace, reset]) {
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
      const context = await browser.newContext();
      const page = await context.newPage();
      const input = scenario({ external: { securityCheck: answer } });
      const backend = await installBackend(context, page, baseURL, input);
      const host = { host: new URL(baseURL!).origin };

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

  test("passes the files of the Monaco code editor on to the network, and fails other files of the same CDN", async ({
    context,
    page,
    baseURL,
  }) => {
    // Stands in for the network, so that the test does not depend on the CDN. The mock passes requests on to it.
    await context.route("https://cdn.jsdelivr.net/**", (route) =>
      route.fulfill({ status: 200, contentType: "text/javascript", body: "// from the CDN" })
    );
    const backend = await installBackend(context, page, baseURL, scenario({}));
    const get = (url: string) =>
      page.evaluate(
        (url) =>
          fetch(url).then(
            (response) => response.text(),
            () => "failed"
          ),
        url
      );

    const monacoLoader = await get("https://cdn.jsdelivr.net/npm/monaco-editor@0.33.0/min/vs/loader.js");
    const otherPackage = await get("https://cdn.jsdelivr.net/npm/left-pad@1.3.0/index.js");

    expect(monacoLoader).toBe("// from the CDN");
    expect(otherPackage).toBe("failed");
    expect(backend.problems()).toEqual([
      "Unexpected request: GET https://cdn.jsdelivr.net/npm/left-pad@1.3.0/index.js (external host)",
    ]);
  });

  test("answers a schema refresh with the onRefresh answer, and fails a refresh that has no answer", async ({
    context,
    page,
    baseURL,
  }) => {
    const input = scenario({ connections: [pokeConnection], experiments: { asyncSchemaDiscovery: false } });
    const backend = await installBackend(context, page, baseURL, input);
    const { connectionId, syncCatalog } = backend.airbyte.store.connections.get(pokeConnection);
    const getConnection = (withRefreshedCatalog: boolean) =>
      send(page, "/api/v1/web_backend/connections/get", "POST", { connectionId, withRefreshedCatalog });
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
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);

    const response = await validateMappers(page, { connectionId });

    expect(JSON.parse(response.text)).toEqual({
      initialFields: [],
      outputFields: [],
      mappers: [{ id: "m1", inputFields: [], outputFields: [] }],
    });
  });

  test("answers a mapper validation with the fields that allValid gives", async ({ context, page, baseURL }) => {
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);
    const fields: FieldSpec[] = [
      { name: "name", type: "STRING", isSelectedPrimaryKey: false, isSelectedCursor: false },
    ];
    backend.airbyte.override("webBackendValidateMappers", mapperValidation.allValid(fields));

    const response = await validateMappers(page, { connectionId });

    expect(JSON.parse(response.text)).toEqual({
      initialFields: fields,
      outputFields: fields,
      mappers: [{ id: "m1", inputFields: fields, outputFields: fields }],
    });
  });

  test("answers 404 to a mapper validation for an unknown connection", async ({ context, page, baseURL }) => {
    await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));

    const response = await validateMappers(page, { connectionId: "c0000000-0000-4000-8000-000000000404" });

    expect(response.status).toBe(404);
  });

  test("fails the test on a mapper validation for a stream the connection does not have", async ({
    context,
    page,
    baseURL,
  }) => {
    const backend = await installBackend(context, page, baseURL, scenario({ connections: [pokeConnection] }));
    const { connectionId } = backend.airbyte.store.connections.get(pokeConnection);

    const response = await validateMappers(page, {
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
    const backend = await installBackend(context, page, baseURL, input);
    const { userId } = backend.airbyte.store.identity.user;
    const getInstance = () => send(page, "/api/v1/instance_configuration", "GET");
    const setupForm = {
      email: "admin@example.com",
      organizationName: "ACME Corp",
      anonymousDataCollection: true,
      initialSetupComplete: true,
      displaySetupWizard: false,
    };

    const before = JSON.parse((await getInstance()).text);
    const setup = await send(page, "/api/v1/instance_configuration/setup", "POST", setupForm);
    const after = JSON.parse((await getInstance()).text);
    const user = JSON.parse((await send(page, "/api/v1/users/get", "POST", { userId })).text);
    const workspaces = JSON.parse((await send(page, "/api/v1/workspaces/list_by_user_id", "POST", { userId })).text);

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

  test("serves the form of a marketplace definition, and saves a source that the webapp creates as a draft", async ({
    context,
    page,
    baseURL,
  }) => {
    const backend = await installBackend(context, page, baseURL, scenario({ sourceDefinitions: [defs.pokeApiSource] }));
    const { workspaceId } = backend.airbyte;
    const { sourceDefinitionId } = defs.pokeApiSource.input.definition;

    const definitions = (await post(page, "source_definitions/list_for_workspace", { workspaceId })).body;
    const specification = (
      await post(page, "source_definition_specifications/get", { workspaceId, sourceDefinitionId })
    ).body;
    const created = (
      await post(page, "sources/create", {
        workspaceId,
        sourceDefinitionId,
        name: "PokeAPI draft",
        connectionConfiguration: { pokemon_name: "ditto" },
        createAsDraft: true,
      })
    ).body;
    const listed = (await post(page, "sources/list", { workspaceId })).body;

    expect(definitions.sourceDefinitions).toEqual([expect.objectContaining({ sourceDefinitionId })]);
    expect(specification.connectionSpecification.required).toEqual(["pokemon_name"]);
    expect(created).toMatchObject({
      sourceId: "50000000-0000-4000-8000-000000000001",
      sourceName: "PokeAPI",
      connectionConfiguration: { pokemon_name: "ditto" },
      isDraft: true,
    });
    expect(listed.sources).toEqual([created]);
    expect(backend.problems()).toEqual([]);
  });

  test("reads, updates and deletes a source, and fails to delete a source that has connections", async ({
    context,
    page,
    baseURL,
  }) => {
    const pokeSource = pokeConnection.input.source;
    const unusedSource = source({ def: defs.pokeApiSource, name: "Unused source" });
    const backend = await installBackend(
      context,
      page,
      baseURL,
      scenario({ sources: [unusedSource], connections: [pokeConnection] })
    );
    const { workspaceId, store } = backend.airbyte;
    const { sourceId } = store.sources.get(pokeSource);
    const unusedSourceId = store.sources.get(unusedSource).sourceId;
    const { connectionId } = store.connections.get(pokeConnection);

    const read = await post(page, "sources/get", { sourceId });
    const specification = await post(page, "source_definition_specifications/get_for_source", { sourceId });
    const connectionsOfSource = await post(page, "web_backend/connections/list", {
      workspaceId,
      sourceId: [sourceId],
    });
    const connectionsOfUnused = await post(page, "web_backend/connections/list", {
      workspaceId,
      sourceId: [unusedSourceId],
    });
    const updated = await post(page, "sources/update", {
      sourceId,
      name: "Renamed",
      connectionConfiguration: { pokemon_name: "ditto" },
    });
    const deleted = await post(page, "sources/delete", { sourceId: unusedSourceId });
    const readAfterDelete = await post(page, "sources/get", { sourceId: unusedSourceId });
    const deletedWithConnections = await post(page, "sources/delete", { sourceId });
    const listed = await post(page, "sources/list", { workspaceId });

    expect(read.body).toMatchObject({ sourceId, isDraft: false, isEntitled: true });
    expect(specification.body).toMatchObject({ sourceDefinitionId: read.body.sourceDefinitionId });
    expect(specification.body.connectionSpecification.required).toEqual(["pokemon_name"]);
    expect(connectionsOfSource.body.connections).toEqual([expect.objectContaining({ connectionId })]);
    expect(connectionsOfUnused.body.connections).toEqual([]);
    expect(updated.body).toMatchObject({
      sourceId,
      name: "Renamed",
      connectionConfiguration: { pokemon_name: "ditto" },
    });
    expect(deleted.status).toBe(204);
    expect(readAfterDelete.status).toBe(404);
    expect(deletedWithConnections.status).toBe(500);
    expect(listed.body.sources.map(({ name }: { name: string }) => name)).toEqual(["Renamed"]);
    expect(backend.problems()).toEqual([
      `Handler error: POST /api/v1/sources/delete (deleteSource): The mock does not delete source ${sourceId}, because it has connections`,
    ]);
  });

  test("promotes a draft source or destination when the webapp reads a succeeded check of it", async ({
    context,
    page,
    baseURL,
  }) => {
    const input = scenario({
      sourceDefinitions: [defs.pokeApiSource],
      destinationDefinitions: [defs.e2eTestingDestination],
    });
    const backend = await installBackend(context, page, baseURL, input);
    const { workspaceId } = backend.airbyte;

    const drafts = await createDrafts(page, workspaceId);
    const sourceOutput = await checkActor(page, "check-1", drafts.sourceId);
    const destinationOutput = await checkActor(page, "check-2", drafts.destinationId);
    const sources = await post(page, "sources/list", { workspaceId });
    const destinations = await post(page, "destinations/list", { workspaceId });

    expect(sourceOutput).toEqual({ id: "check-1", status: "succeeded" });
    expect(destinationOutput).toEqual({ id: "check-2", status: "succeeded" });
    expect(sources.body.sources).toEqual([expect.objectContaining({ sourceId: drafts.sourceId, isDraft: false })]);
    expect(destinations.body.destinations).toEqual([
      expect.objectContaining({ destinationId: drafts.destinationId, isDraft: false }),
    ]);
    expect(backend.problems()).toEqual([]);
  });

  test("keeps a draft source or destination a draft when the webapp reads a failed check of it", async ({
    context,
    page,
    baseURL,
  }) => {
    const input = scenario({
      sourceDefinitions: [defs.pokeApiSource],
      destinationDefinitions: [defs.e2eTestingDestination],
      outcomes: { check: checkOutput.failed({ message: "Bad credentials" }) },
    });
    const backend = await installBackend(context, page, baseURL, input);
    const { workspaceId } = backend.airbyte;

    const drafts = await createDrafts(page, workspaceId);
    const sourceOutput = await checkActor(page, "check-1", drafts.sourceId);
    const destinationOutput = await checkActor(page, "check-2", drafts.destinationId);
    const sources = await post(page, "sources/list", { workspaceId });
    const destinations = await post(page, "destinations/list", { workspaceId });

    expect(sourceOutput).toMatchObject({ id: "check-1", status: "failed", message: "Bad credentials" });
    expect(destinationOutput).toMatchObject({ id: "check-2", status: "failed", message: "Bad credentials" });
    expect(sources.body.sources).toEqual([expect.objectContaining({ sourceId: drafts.sourceId, isDraft: true })]);
    expect(destinations.body.destinations).toEqual([
      expect.objectContaining({ destinationId: drafts.destinationId, isDraft: true }),
    ]);
    expect(backend.problems()).toEqual([]);
  });
});
