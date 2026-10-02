import type { MockAirbyteScenario } from "./mockApiState";
import type { BrowserContext, Route } from "@playwright/test";
import type {
  WebBackendConnectionRead,
  WebBackendConnectionUpdate,
  WorkspaceReadList,
  InstanceConfigurationResponse,
  HealthCheckRead,
  OrganizationInfoRead,
  OrganizationReadList,
  ListOrganizationSummariesResponse,
  PermissionReadList,
  WebBackendCheckUpdatesRead,
  ConnectionStatusesRead,
  StreamStatusReadList,
  ConnectionLastJobPerStreamRead,
  WebBackendCronExpressionDescription,
  ConnectionState,
  DestinationDefinitionSpecificationRead,
  DataplaneGroupListResponse,
  Tag,
  SourceDefinitionSpecificationRead,
  SourceReadList,
  DestinationReadList,
  SourceDefinitionReadList,
  DestinationDefinitionReadList,
  WebBackendConnectionCreate,
  WebBackendConnectionReadList,
  RunDiscoverCommand200,
  GetCommandStatus200,
  GetDiscoverCommandOutput200,
  CancelCommand200,
  WebBackendConnectionStatusCounts,
  ConnectionEventListMinimal,
} from "@src/core/api/types/AirbyteClient";

import { expect } from "@playwright/test";

import { createMockAirbyteState } from "./mockApiState";

export type { MockAirbyteScenario } from "./mockApiState";

export function createMockAirbyte(scenario: MockAirbyteScenario) {
  const {
    seed,
    user,
    workspace,
    source,
    destination,
    connection,
    catalog,
    catalogId,
    connections,
    sourceDefinition,
    destinationDefinition,
  } = createMockAirbyteState(scenario);
  const creations: WebBackendConnectionCreate[] = [];
  const commands = new Set<string>();
  const updates: WebBackendConnectionUpdate[] = [];
  const requests: Array<{ key: string; body: Record<string, unknown> }> = [];
  const unexpectedRequests: string[] = [];

  type ApiHandler = (route: Route, body: Record<string, unknown>) => Promise<void>;

  function getCommandId(body: Record<string, unknown>) {
    expect(commands.has(body.id as string), `Unknown discovery command ID: ${body.id}`).toBe(true);
    return body.id as string;
  }

  function getConnection(connectionId: unknown) {
    const saved = connections.find((connection) => connection.connectionId === connectionId);
    expect(saved, `Unknown connection ID: ${connectionId}`).toBeDefined();
    return saved!;
  }

  const discoveryHandlers: Record<string, ApiHandler> = {
    "POST /api/v1/commands/run/discover": async (route, body) => {
      expect(body.actor_id).toBe(source.sourceId);
      expect(typeof body.id).toBe("string");
      expect(body.id).not.toBe("");
      commands.add(body.id as string);
      await route.fulfill({ json: { id: body.id as string } satisfies RunDiscoverCommand200 });
    },
    "POST /api/v1/commands/status": (route, body) =>
      route.fulfill({
        json: { id: getCommandId(body), status: "completed" } satisfies GetCommandStatus200,
      }),
    "POST /api/v1/commands/output/discover": (route, body) =>
      route.fulfill({
        json: { id: getCommandId(body), catalog, catalogId, status: "succeeded" } satisfies GetDiscoverCommandOutput200,
      }),
    "POST /api/v1/commands/cancel": (route, body) =>
      route.fulfill({ json: { id: getCommandId(body) } satisfies CancelCommand200 }),
  };

  const connectionHandlers: Record<string, ApiHandler> = {
    "POST /api/v1/web_backend/connections/status_counts": async (route, body) => {
      expect(body.workspaceId).toBe(workspace.workspaceId);
      await route.fulfill({
        json: {
          running: connections.filter(({ isSyncing }) => isSyncing).length,
          queued: 0,
          healthy: connections.filter(({ latestSyncJobStatus }) => latestSyncJobStatus === "succeeded").length,
          failed: connections.filter(({ latestSyncJobStatus }) => latestSyncJobStatus === "failed").length,
          paused: connections.filter(({ status }) => status === "inactive").length,
          notSynced: connections.filter(({ latestSyncJobStatus }) => !latestSyncJobStatus).length,
        } satisfies WebBackendConnectionStatusCounts,
      });
    },
    "POST /api/v1/connections/events/list_minimal": async (route, body) => {
      expect(body.workspaceId).toBe(workspace.workspaceId);
      await route.fulfill({ json: { events: [] } satisfies ConnectionEventListMinimal });
    },
    "POST /api/v1/web_backend/connections/get": async (route, body) => {
      const saved = getConnection(body.connectionId);
      expect(body.withRefreshedCatalog).toBe(false);
      await route.fulfill({ json: saved });
    },
    "POST /api/v1/web_backend/connections/list": async (route, body) => {
      expect(body.workspaceId).toBe(workspace.workspaceId);
      await route.fulfill({
        json: {
          connections,
          page_size: Number(body.pageSize ?? 25),
          num_connections: connections.length,
        } satisfies WebBackendConnectionReadList,
      });
    },
    "POST /api/v1/connections/status": async (route, body) => {
      expect(Array.isArray(body.connectionIds)).toBe(true);
      const statuses: ConnectionStatusesRead = (body.connectionIds as string[]).map((connectionId) => {
        getConnection(connectionId);
        return { connectionId, connectionSyncStatus: "pending" };
      });
      await route.fulfill({ json: statuses });
    },
    "POST /api/v1/web_backend/connections/create": async (route, body) => {
      expect(body.sourceId).toBe(source.sourceId);
      expect(body.destinationId).toBe(destination.destinationId);
      expect(body.sourceCatalogId).toBe(catalogId);
      const creation = body as unknown as WebBackendConnectionCreate;
      expect(creation.operations ?? []).toEqual([]);
      expect(creation.syncCatalog?.streams.some(({ config }) => config?.selected)).toBe(true);

      const saved: WebBackendConnectionRead = {
        ...connection,
        ...creation,
        operations: [],
        connectionId: `a9c8e4b5-349d-4a17-bdff-${String(connections.length + 1).padStart(12, "0")}`,
        name: creation.name ?? `${source.name} → ${destination.name}`,
        syncCatalog: creation.syncCatalog!,
        catalogId,
      };
      creations.push(creation);
      connections.push(saved);
      await route.fulfill({ json: saved });
    },
    "POST /api/v1/web_backend/connections/update": async (route, body) => {
      const saved = getConnection(body.connectionId);
      expect(["cron", "basic"]).toContain(body.scheduleType);
      expect(body.scheduleData).toBeTruthy();
      const update = body as unknown as WebBackendConnectionUpdate;
      if (update.scheduleType === "cron") {
        expect(update.scheduleData?.cron).toEqual({ cronExpression: "0 0 12 * * ?", cronTimeZone: "UTC" });
      } else {
        expect(update.scheduleData?.basicSchedule).toEqual({ timeUnit: "hours", units: 1 });
      }

      updates.push(update);
      saved.scheduleType = update.scheduleType;
      saved.scheduleData = update.scheduleData;
      await route.fulfill({ json: saved });
    },
    "POST /api/v1/web_backend/describe_cron_expression": async (route, body) => {
      expect(body).toEqual({ cronExpression: "0 0 12 * * ?" });
      await route.fulfill({
        json: {
          cronExpression: "0 0 12 * * ?",
          description: "At 12:00 PM",
          nextExecutions: [1791028800, 1791115200],
        } satisfies WebBackendCronExpressionDescription,
      });
    },
  };

  async function install(context: BrowserContext, baseURL: string) {
    const responses: Record<string, unknown> = {
      "GET /api/v1/web_backend/config": { version: "dev", edition: "community" },
      "GET /api/v1/health": { available: true } satisfies HealthCheckRead,
      "GET /api/v1/instance_configuration": {
        edition: "community",
        version: "dev",
        auth: { mode: "none" },
        airbyteUrl: baseURL,
        initialSetupComplete: true,
        defaultUserId: user.userId,
        defaultOrganizationId: workspace.organizationId,
        defaultOrganizationEmail: user.email,
        defaultWorkspaceId: workspace.workspaceId,
        trackingStrategy: "logging",
      } satisfies InstanceConfigurationResponse,
      "POST /api/v1/users/get": user,
      "POST /api/v1/web_backend/check_updates": {
        sourceDefinitions: 0,
        destinationDefinitions: 0,
      } satisfies WebBackendCheckUpdatesRead,
      "POST /api/v1/workspaces/get": workspace,
      "POST /api/v1/workspaces/list_by_organization_id": { workspaces: [workspace] } satisfies WorkspaceReadList,
      "POST /api/v1/organizations/get_organization_info": {
        organizationId: workspace.organizationId,
        organizationName: "Test organization",
        sso: false,
        scim: false,
      } satisfies OrganizationInfoRead,
      "POST /api/v1/organizations/list_summaries": {
        organizationSummaries: [
          {
            organization: {
              organizationId: workspace.organizationId,
              organizationName: "Test organization",
              email: user.email,
            },
            workspaces: [workspace],
            memberCount: 1,
          },
        ],
      } satisfies ListOrganizationSummariesResponse,
      "POST /api/v1/organizations/list_by_user_id": {
        organizations: [
          {
            organizationId: workspace.organizationId,
            organizationName: "Test organization",
            email: user.email,
          },
        ],
      } satisfies OrganizationReadList,
      "POST /api/v1/permissions/list_by_user": {
        permissions: [
          {
            permissionId: "test-permission",
            permissionType: "organization_admin",
            userId: user.userId,
            organizationId: workspace.organizationId,
          },
        ],
      } satisfies PermissionReadList,
      "POST /api/v1/actor_definition_versions/get_for_source": connection.sourceActorDefinitionVersion,
      "POST /api/v1/actor_definition_versions/get_for_destination": connection.destinationActorDefinitionVersion,
      "POST /api/v1/source_definitions/get_for_workspace": sourceDefinition,
      "POST /api/v1/destination_definitions/get_for_workspace": destinationDefinition,
      "POST /api/v1/stream_statuses/latest_per_run_state": { streamStatuses: [] } satisfies StreamStatusReadList,
      "POST /api/v1/connections/last_job_per_stream": [] satisfies ConnectionLastJobPerStreamRead,
      "POST /api/v1/state/get": {
        connectionId: connection.connectionId,
        stateType: "not_set",
      } satisfies ConnectionState,
      "POST /api/v1/destination_definition_specifications/get_for_destination": {
        destinationDefinitionId: destination.destinationDefinitionId,
        documentationUrl: "",
        connectionSpecification: seed.destination.specification,
        supportedDestinationSyncModes: seed.destination.supportedSyncModes,
        jobInfo: { id: "spec-job", configType: "get_spec", createdAt: 0, endedAt: 0, succeeded: true },
      } satisfies DestinationDefinitionSpecificationRead,
      "POST /api/v1/dataplane_group/list": { dataplaneGroups: [] } satisfies DataplaneGroupListResponse,
      "POST /api/v1/tags/list": [] satisfies Tag[],
      "POST /api/v1/sources/list": { sources: [source] } satisfies SourceReadList,
      "POST /api/v1/destinations/list": { destinations: [destination] } satisfies DestinationReadList,
      "POST /api/v1/sources/get": source,
      "POST /api/v1/destinations/get": destination,
      "POST /api/v1/source_definition_specifications/get_for_source": {
        sourceDefinitionId: source.sourceDefinitionId,
        connectionSpecification: seed.source.specification,
        jobInfo: { id: "spec-job", configType: "get_spec", createdAt: 0, endedAt: 0, succeeded: true },
      } satisfies SourceDefinitionSpecificationRead,
      "POST /api/v1/source_definitions/list_for_workspace": {
        sourceDefinitions: [sourceDefinition],
      } satisfies SourceDefinitionReadList,
      "POST /api/v1/destination_definitions/list_for_workspace": {
        destinationDefinitions: [destinationDefinition],
      } satisfies DestinationDefinitionReadList,
      "POST /api/v1/source_definitions/list_enterprise_stubs_for_workspace": { enterpriseConnectorStubs: [] },
      "POST /api/v1/destination_definitions/list_enterprise_stubs_for_workspace": { enterpriseConnectorStubs: [] },
    };
    const targetIds: Record<string, Record<string, string>> = {
      "users/get": { userId: user.userId },
      "workspaces/get": { workspaceId: workspace.workspaceId },
      "workspaces/list_by_organization_id": { organizationId: workspace.organizationId! },
      "organizations/get_organization_info": { organizationId: workspace.organizationId! },
      "organizations/list_by_user_id": { userId: user.userId },
      "organizations/list_summaries": { userId: user.userId },
      "permissions/list_by_user": { userId: user.userId },
      "actor_definition_versions/get_for_source": { sourceId: source.sourceId },
      "actor_definition_versions/get_for_destination": { destinationId: destination.destinationId },
      "source_definitions/get_for_workspace": {
        workspaceId: workspace.workspaceId,
        sourceDefinitionId: source.sourceDefinitionId,
      },
      "destination_definitions/get_for_workspace": {
        workspaceId: workspace.workspaceId,
        destinationDefinitionId: destination.destinationDefinitionId,
      },
      "destination_definition_specifications/get_for_destination": { destinationId: destination.destinationId },
      "dataplane_group/list": { organization_id: workspace.organizationId! },
      "tags/list": { workspaceId: workspace.workspaceId },
      "sources/list": { workspaceId: workspace.workspaceId },
      "destinations/list": { workspaceId: workspace.workspaceId },
      "sources/get": { sourceId: source.sourceId },
      "destinations/get": { destinationId: destination.destinationId },
      "source_definition_specifications/get_for_source": { sourceId: source.sourceId },
      "source_definitions/list_for_workspace": { workspaceId: workspace.workspaceId },
      "destination_definitions/list_for_workspace": { workspaceId: workspace.workspaceId },
      "source_definitions/list_enterprise_stubs_for_workspace": { workspaceId: workspace.workspaceId },
      "destination_definitions/list_enterprise_stubs_for_workspace": { workspaceId: workspace.workspaceId },
    };
    const handlers = { ...discoveryHandlers, ...connectionHandlers };
    await context.route(
      (url) => url.pathname.startsWith("/api/") || url.origin !== new URL(baseURL).origin,
      async (route) => {
        const request = route.request();
        const url = new URL(request.url());
        const key = `${request.method()} ${url.pathname}`;
        if (!url.pathname.startsWith("/api/")) {
          unexpectedRequests.push(key);
          await route.abort();
          throw new Error(`Unexpected external request: ${request.url()}`);
        }

        const body: Record<string, unknown> = request.postData() ? request.postDataJSON() : {};
        requests.push({ key, body });
        const handler = handlers[key];
        if (!handler && !(key in responses)) {
          unexpectedRequests.push(key);
          await route.abort();
          throw new Error(`Unexpected API request: ${key}`);
        }

        const endpoint = url.pathname.replace("/api/v1/", "");
        for (const [field, expected] of Object.entries(targetIds[endpoint] ?? {})) {
          expect(body[field], `${key}: ${field}`).toBe(expected);
        }

        if (
          ["stream_statuses/latest_per_run_state", "connections/last_job_per_stream", "state/get"].includes(endpoint)
        ) {
          getConnection(body.connectionId);
          if (endpoint === "state/get") {
            await route.fulfill({
              json: { connectionId: body.connectionId as string, stateType: "not_set" } satisfies ConnectionState,
            });
            return;
          }
        }

        if (handler) {
          await handler(route, body);
          return;
        }

        await route.fulfill({ json: responses[key] });
      }
    );
  }

  return {
    user,
    workspace,
    source,
    destination,
    connections,
    connection,
    creations,
    updates,
    requests,
    install,
    assertNoUnexpectedRequests: () => expect(unexpectedRequests).toEqual([]),
  };
}

export type MockAirbyte = ReturnType<typeof createMockAirbyte>;
