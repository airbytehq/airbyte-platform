import type { ApiHandler } from "./mockApiHandlers";
import type { MockAirbyteScenario } from "./mockApiState";
import type { BrowserContext } from "@playwright/test";
import type {
  WebBackendConnectionUpdate,
  WorkspaceReadList,
  InstanceConfigurationResponse,
  HealthCheckRead,
  OrganizationInfoRead,
  OrganizationReadList,
  ListOrganizationSummariesResponse,
  PermissionReadList,
  WebBackendCheckUpdatesRead,
  DestinationDefinitionSpecificationRead,
  DataplaneGroupListResponse,
  Tag,
  SourceDefinitionSpecificationRead,
  SourceReadList,
  DestinationReadList,
  SourceDefinitionReadList,
  DestinationDefinitionReadList,
  WebBackendConnectionCreate,
  RunDiscoverCommand200,
  GetCommandStatus200,
  GetDiscoverCommandOutput200,
  CancelCommand200,
} from "@src/core/api/types/AirbyteClient";

import { expect } from "@playwright/test";

import { createMockAirbyteState } from "./mockApiState";
import { createConnectionHandlers } from "./mockConnectionHandlers";
import { createJobHandlers } from "./mockJobHandlers";
import { createMockJobs } from "./mockJobs";

export type { MockAirbyteScenario } from "./mockApiState";

export function createMockAirbyte(scenario: MockAirbyteScenario) {
  const state = createMockAirbyteState(scenario);
  const {
    seed,
    user,
    workspace,
    source,
    destination,
    connectionTemplate,
    catalog,
    catalogId,
    sourceDefinition,
    destinationDefinition,
  } = state;
  const creations: WebBackendConnectionCreate[] = [];
  const commands = new Set<string>();
  const updates: WebBackendConnectionUpdate[] = [];
  const requests: Array<{ key: string; body: Record<string, unknown> }> = [];
  const unexpectedRequests: string[] = [];

  function getCommandId(body: Record<string, unknown>) {
    const commandId = body.id as string;
    expect(commands.has(commandId), `Unknown discovery command ID: ${commandId}`).toBe(true);
    return commandId;
  }

  const discoveryHandlers: Record<string, ApiHandler> = {
    "POST /api/v1/commands/run/discover": (body) => {
      expect(body.actor_id).toBe(source.sourceId);
      expect(typeof body.id).toBe("string");
      expect(body.id).not.toBe("");
      commands.add(body.id as string);
      return { json: { id: body.id as string } satisfies RunDiscoverCommand200 };
    },
    "POST /api/v1/commands/status": (body) => ({
      json: { id: getCommandId(body), status: "completed" } satisfies GetCommandStatus200,
    }),
    "POST /api/v1/commands/output/discover": (body) => ({
      json: { id: getCommandId(body), catalog, catalogId, status: "succeeded" } satisfies GetDiscoverCommandOutput200,
    }),
    "POST /api/v1/commands/cancel": (body) => ({ json: { id: getCommandId(body) } satisfies CancelCommand200 }),
  };

  const jobs = createMockJobs({ onJobChanged: state.updateConnectionFromJob });
  const connectionHandlers = createConnectionHandlers(state, { creations, updates }, jobs);
  const jobHandlers = createJobHandlers(jobs);

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
      "POST /api/v1/actor_definition_versions/get_for_source": connectionTemplate.sourceActorDefinitionVersion,
      "POST /api/v1/actor_definition_versions/get_for_destination":
        connectionTemplate.destinationActorDefinitionVersion,
      "POST /api/v1/source_definitions/get_for_workspace": sourceDefinition,
      "POST /api/v1/destination_definitions/get_for_workspace": destinationDefinition,
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
    const handlers = { ...discoveryHandlers, ...connectionHandlers, ...jobHandlers };
    const baseOrigin = new URL(baseURL).origin;
    await context.route(
      (url) => url.pathname.startsWith("/api/") || url.origin !== baseOrigin,
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

        const response = handler ? await handler(body) : { json: responses[key] };
        await route.fulfill(response);
      }
    );
  }

  return {
    user,
    workspace,
    source,
    destination,
    get connections() {
      return state.connections;
    },
    jobs,
    connectionTemplate,
    creations,
    updates,
    requests,
    install,
    assertNoUnexpectedRequests: () => expect(unexpectedRequests).toEqual([]),
  };
}

export type MockAirbyte = ReturnType<typeof createMockAirbyte>;
