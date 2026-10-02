import type { BrowserContext } from "@playwright/test";
import type {
  SourceRead,
  DestinationRead,
  WebBackendConnectionRead,
  WebBackendConnectionUpdate,
  WorkspaceRead,
  UserRead,
  InstanceConfigurationResponse,
  HealthCheckRead,
  OrganizationInfoRead,
  OrganizationReadList,
  ListOrganizationSummariesResponse,
  PermissionReadList,
  WebBackendCheckUpdatesRead,
  SourceDefinitionRead,
  DestinationDefinitionRead,
  ConnectionStatusesRead,
  StreamStatusReadList,
  ConnectionLastJobPerStreamRead,
  WebBackendCronExpressionDescription,
  ConnectionState,
  DestinationDefinitionSpecificationRead,
  DataplaneGroupListResponse,
  Tag,
} from "@src/core/api/types/AirbyteClient";

import { expect } from "@playwright/test";
import destinationIds from "@src/area/connector/utils/destinations.json";
import sourceIds from "@src/area/connector/utils/sources.json";

export function createMockAirbyte() {
  const user: UserRead = {
    userId: "43bcf34b-7e49-424e-a758-3fdd10a73699",
    email: "test@example.com",
    name: "Test User",
    metadata: {},
  };
  const workspace: WorkspaceRead = {
    workspaceId: "47c74b9b-9b89-4af1-8331-4865af6c4e4d",
    customerId: "55dd55e2-33ac-44dc-8d65-5aa7c8624f72",
    organizationId: "db338860-f35c-4b5b-b57f-5171070bdcd9",
    email: user.email,
    name: "Test workspace",
    slug: "test-workspace",
    initialSetupComplete: true,
    displaySetupWizard: false,
    anonymousDataCollection: false,
  };
  const source: SourceRead = {
    sourceId: "b5117460-9237-4fd0-b6a9-45ac22a6db59",
    sourceDefinitionId: sourceIds.Faker,
    workspaceId: workspace.workspaceId,
    name: "Faker source",
    sourceName: "Faker",
    connectionConfiguration: { count: 10, seed: 12345 },
    createdAt: 0,
    isDraft: false,
  };
  const destination: DestinationRead = {
    destinationId: "35df745e-6e2c-4e83-b9d1-36f560403d2a",
    destinationDefinitionId: destinationIds.EndToEndTesting,
    workspaceId: workspace.workspaceId,
    name: "E2E Testing destination",
    destinationName: "End-to-End Testing",
    connectionConfiguration: {},
    createdAt: 0,
    isDraft: false,
  };
  const connection: WebBackendConnectionRead = {
    connectionId: "a9c8e4b5-349d-4a17-bdff-5ad2f6fbd611",
    name: "Faker source → E2E Testing destination",
    sourceId: source.sourceId,
    destinationId: destination.destinationId,
    source,
    destination,
    syncCatalog: {
      streams: [
        {
          stream: {
            name: "users",
            jsonSchema: { type: "object", properties: { id: { type: "integer" } } },
            supportedSyncModes: ["full_refresh"],
          },
          config: { syncMode: "full_refresh", destinationSyncMode: "overwrite", selected: true },
        },
      ],
    },
    scheduleType: "manual",
    status: "active",
    isSyncing: false,
    schemaChange: "no_change",
    notifySchemaChanges: true,
    notifySchemaChangesByEmail: false,
    nonBreakingChangesPreference: "ignore",
    sourceActorDefinitionVersion: {
      dockerRepository: "airbyte/source-faker",
      dockerImageTag: "1.0.0",
      supportsRefreshes: true,
      isVersionOverrideApplied: false,
      supportState: "supported",
      supportsFileTransfer: false,
      supportsDataActivation: false,
    },
    destinationActorDefinitionVersion: {
      dockerRepository: "airbyte/destination-e2e-test",
      dockerImageTag: "1.0.0",
      supportsRefreshes: true,
      isVersionOverrideApplied: false,
      supportState: "supported",
      supportsFileTransfer: false,
      supportsDataActivation: false,
    },
    tags: [],
    onDemandEnabled: false,
  };

  const updates: WebBackendConnectionUpdate[] = [];
  const requests: Array<{ key: string; body: Record<string, unknown> }> = [];
  const unexpectedRequests: string[] = [];

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
      "POST /api/v1/users/get": user satisfies UserRead,
      "POST /api/v1/web_backend/check_updates": {
        sourceDefinitions: 0,
        destinationDefinitions: 0,
      } satisfies WebBackendCheckUpdatesRead,
      "POST /api/v1/workspaces/get": workspace satisfies WorkspaceRead,
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
      "POST /api/v1/source_definitions/get_for_workspace": {
        sourceDefinitionId: source.sourceDefinitionId,
        name: source.sourceName,
        ...connection.sourceActorDefinitionVersion,
      } satisfies SourceDefinitionRead,
      "POST /api/v1/destination_definitions/get_for_workspace": {
        destinationDefinitionId: destination.destinationDefinitionId,
        name: destination.destinationName,
        documentationUrl: "",
        ...connection.destinationActorDefinitionVersion,
      } satisfies DestinationDefinitionRead,
      "POST /api/v1/connections/status": [
        { connectionId: connection.connectionId, connectionSyncStatus: "pending" },
      ] satisfies ConnectionStatusesRead,
      "POST /api/v1/stream_statuses/latest_per_run_state": { streamStatuses: [] } satisfies StreamStatusReadList,
      "POST /api/v1/connections/last_job_per_stream": [] satisfies ConnectionLastJobPerStreamRead,
      "POST /api/v1/state/get": {
        connectionId: connection.connectionId,
        stateType: "not_set",
      } satisfies ConnectionState,
      "POST /api/v1/destination_definition_specifications/get_for_destination": {
        destinationDefinitionId: destination.destinationDefinitionId,
        documentationUrl: "",
        connectionSpecification: { type: "object", properties: {} },
        supportedDestinationSyncModes: ["overwrite"],
        jobInfo: { id: "spec-job", configType: "get_spec", createdAt: 0, endedAt: 0, succeeded: true },
      } satisfies DestinationDefinitionSpecificationRead,
      "POST /api/v1/dataplane_group/list": { dataplaneGroups: [] } satisfies DataplaneGroupListResponse,
      "POST /api/v1/tags/list": [] satisfies Tag[],
    };
    const targetIds: Record<string, Record<string, string>> = {
      "users/get": { userId: user.userId },
      "workspaces/get": { workspaceId: workspace.workspaceId },
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
      "stream_statuses/latest_per_run_state": {
        connectionId: connection.connectionId,
      },
      "connections/last_job_per_stream": { connectionId: connection.connectionId },
      "state/get": { connectionId: connection.connectionId },
      "destination_definition_specifications/get_for_destination": { destinationId: destination.destinationId },
      "dataplane_group/list": { organization_id: workspace.organizationId! },
      "tags/list": { workspaceId: workspace.workspaceId },
    };
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
        const endpoint = url.pathname.replace("/api/v1/", "");
        for (const [field, expected] of Object.entries(targetIds[endpoint] ?? {})) {
          expect(body[field], `${key}: ${field}`).toBe(expected);
        }

        if (key === "POST /api/v1/web_backend/connections/get") {
          expect(body).toEqual({ connectionId: connection.connectionId, withRefreshedCatalog: false });
          await route.fulfill({ json: connection });
          return;
        }

        if (key === "POST /api/v1/web_backend/connections/update") {
          expect(body.connectionId).toBe(connection.connectionId);
          expect(["cron", "basic"]).toContain(body.scheduleType);
          expect(body.scheduleData).toBeTruthy();
          const update = body as unknown as WebBackendConnectionUpdate;
          if (update.scheduleType === "cron") {
            expect(update.scheduleData?.cron).toEqual({ cronExpression: "0 0 12 * * ?", cronTimeZone: "UTC" });
          } else {
            expect(update.scheduleData?.basicSchedule).toEqual({ timeUnit: "hours", units: 1 });
          }

          updates.push(update);
          connection.scheduleType = update.scheduleType;
          connection.scheduleData = update.scheduleData;
          await route.fulfill({ json: connection });
          return;
        }

        if (key === "POST /api/v1/web_backend/describe_cron_expression") {
          expect(body).toEqual({ cronExpression: "0 0 12 * * ?" });
          await route.fulfill({
            json: {
              cronExpression: "0 0 12 * * ?",
              description: "At 12:00 PM",
              nextExecutions: [1791028800, 1791115200],
            } satisfies WebBackendCronExpressionDescription,
          });
          return;
        }

        if (!(key in responses)) {
          unexpectedRequests.push(key);
          await route.abort();
          throw new Error(`Unexpected API request: ${key}`);
        }
        if (key === "POST /api/v1/connections/status") {
          expect(body).toEqual({ connectionIds: [connection.connectionId] });
        }
        await route.fulfill({ json: responses[key] });
      }
    );
  }

  return {
    user,
    workspace,
    connection,
    updates,
    requests,
    install,
    assertNoUnexpectedRequests: () => expect(unexpectedRequests).toEqual([]),
  };
}

export type MockAirbyte = ReturnType<typeof createMockAirbyte>;
