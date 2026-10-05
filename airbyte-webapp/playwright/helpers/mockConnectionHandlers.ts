import type { ApiHandler } from "./mockApiHandlers";
import type { MockAirbyteState } from "./mockApiState";
import type {
  ConnectionEventList,
  ConnectionEventListMinimal,
  ConnectionEventsListMinimalRequestBody,
  ConnectionEventsRequestBody,
  ConnectionIdRequestBody,
  ConnectionLastJobPerStreamRead,
  ConnectionState,
  ConnectionStatusesRead,
  ConnectionStatusesRequestBody,
  StreamStatusReadList,
  WebBackendConnectionCreate,
  WebBackendConnectionListRequestBody,
  WebBackendConnectionRead,
  WebBackendConnectionReadList,
  WebBackendConnectionRequestBody,
  WebBackendConnectionStatusCounts,
  WebBackendConnectionUpdate,
  WebBackendCronExpressionDescription,
  WebBackendDescribeCronExpressionRequestBody,
  WorkspaceIdRequestBody,
} from "@src/core/api/types/AirbyteClient";

import { expect } from "@playwright/test";
import { ConnectionScheduleType } from "@src/core/api/types/AirbyteClient";

import { handleRequest } from "./mockApiHandlers";

export function createConnectionHandlers(
  state: MockAirbyteState,
  calls: { creations: WebBackendConnectionCreate[]; updates: WebBackendConnectionUpdate[] }
): Record<string, ApiHandler> {
  const { workspace, source, destination, connectionTemplate, catalogId, connections } = state;

  function requireConnection(connectionId: unknown) {
    const saved = connections.find((connection) => connection.connectionId === connectionId);
    expect(saved, `Unknown connection ID: ${connectionId}`).toBeDefined();
    return saved!;
  }

  function getNonDeprecatedConnections() {
    return connections.filter(({ status }) => status !== "deprecated");
  }

  function getConnectionStatusCounts(body: WorkspaceIdRequestBody) {
    expect(body.workspaceId).toBe(workspace.workspaceId);
    const listedConnections = getNonDeprecatedConnections();
    return {
      json: {
        running: listedConnections.filter(({ isSyncing }) => isSyncing).length,
        queued: 0,
        healthy: listedConnections.filter(({ latestSyncJobStatus }) => latestSyncJobStatus === "succeeded").length,
        failed: listedConnections.filter(({ latestSyncJobStatus }) => latestSyncJobStatus === "failed").length,
        paused: listedConnections.filter(({ status }) => status === "inactive").length,
        notSynced: listedConnections.filter(({ latestSyncJobStatus }) => !latestSyncJobStatus).length,
      } satisfies WebBackendConnectionStatusCounts,
    };
  }

  function listMinimalConnectionEvents(body: ConnectionEventsListMinimalRequestBody) {
    expect(body.workspaceId).toBe(workspace.workspaceId);
    return { json: { events: [] } satisfies ConnectionEventListMinimal };
  }

  function getConnection(body: WebBackendConnectionRequestBody) {
    const saved = requireConnection(body.connectionId);
    expect(body.withRefreshedCatalog).toBe(false);
    return { json: saved satisfies WebBackendConnectionRead };
  }

  function listConnections(body: WebBackendConnectionListRequestBody) {
    expect(body.workspaceId).toBe(workspace.workspaceId);
    const listedConnections = getNonDeprecatedConnections();
    return {
      json: {
        connections: listedConnections,
        page_size: Number(body.pageSize ?? 25),
        num_connections: listedConnections.length,
      } satisfies WebBackendConnectionReadList,
    };
  }

  function getConnectionStatuses(body: ConnectionStatusesRequestBody) {
    expect(Array.isArray(body.connectionIds)).toBe(true);
    const statuses: ConnectionStatusesRead = body.connectionIds.map((connectionId) => {
      requireConnection(connectionId);
      return { connectionId, connectionSyncStatus: "pending" };
    });
    return { json: statuses };
  }

  function getStreamStatuses(body: ConnectionIdRequestBody) {
    requireConnection(body.connectionId);
    return { json: { streamStatuses: [] } satisfies StreamStatusReadList };
  }

  function getLastJobPerStream(body: ConnectionIdRequestBody) {
    requireConnection(body.connectionId);
    return { json: [] satisfies ConnectionLastJobPerStreamRead };
  }

  function getConnectionState(body: ConnectionIdRequestBody) {
    const saved = requireConnection(body.connectionId);
    return { json: { connectionId: saved.connectionId, stateType: "not_set" } satisfies ConnectionState };
  }

  function createConnection(creation: WebBackendConnectionCreate) {
    expect(creation.sourceId).toBe(source.sourceId);
    expect(creation.destinationId).toBe(destination.destinationId);
    expect(creation.sourceCatalogId).toBe(catalogId);
    expect(creation.operations ?? []).toEqual([]);
    expect(creation.syncCatalog?.streams.some(({ config }) => config?.selected)).toBe(true);

    const saved: WebBackendConnectionRead = {
      ...connectionTemplate,
      ...creation,
      operations: [],
      connectionId: `a9c8e4b5-349d-4a17-bdff-${String(connections.length + 1).padStart(12, "0")}`,
      name: creation.name ?? `${source.name} → ${destination.name}`,
      syncCatalog: creation.syncCatalog!,
      catalogId,
    };
    calls.creations.push(creation);
    connections.push(saved);
    return { json: saved satisfies WebBackendConnectionRead };
  }

  function updateConnection(update: WebBackendConnectionUpdate) {
    const saved = requireConnection(update.connectionId);
    expect(saved.status).not.toBe("deprecated");
    assertValidScheduleUpdate(update);

    calls.updates.push(update);
    applyConnectionUpdate(saved, update);
    return { json: saved satisfies WebBackendConnectionRead };
  }

  function deleteConnection(body: ConnectionIdRequestBody) {
    const saved = requireConnection(body.connectionId);
    expect(body).toEqual({ connectionId: saved.connectionId });
    expect(saved.status).not.toBe("deprecated");
    saved.status = "deprecated";
    return { status: 204 };
  }

  function listConnectionEvents(body: ConnectionEventsRequestBody) {
    requireConnection(body.connectionId);
    expect(body.pagination).toEqual({ pageSize: 50, rowOffset: 0 });
    return { json: { events: [] } satisfies ConnectionEventList };
  }

  function describeCronExpression(body: WebBackendDescribeCronExpressionRequestBody) {
    expect(body).toEqual({ cronExpression: "0 0 12 * * ?" });
    return {
      json: {
        cronExpression: "0 0 12 * * ?",
        description: "At 12:00 PM",
        nextExecutions: [1791028800, 1791115200],
      } satisfies WebBackendCronExpressionDescription,
    };
  }

  return {
    "POST /api/v1/web_backend/connections/status_counts": handleRequest(getConnectionStatusCounts),
    "POST /api/v1/connections/events/list_minimal": handleRequest(listMinimalConnectionEvents),
    "POST /api/v1/web_backend/connections/get": handleRequest(getConnection),
    "POST /api/v1/web_backend/connections/list": handleRequest(listConnections),
    "POST /api/v1/connections/status": handleRequest(getConnectionStatuses),
    "POST /api/v1/stream_statuses/latest_per_run_state": handleRequest(getStreamStatuses),
    "POST /api/v1/connections/last_job_per_stream": handleRequest(getLastJobPerStream),
    "POST /api/v1/state/get": handleRequest(getConnectionState),
    "POST /api/v1/web_backend/connections/create": handleRequest(createConnection),
    "POST /api/v1/web_backend/connections/update": handleRequest(updateConnection),
    "POST /api/v1/connections/delete": handleRequest(deleteConnection),
    "POST /api/v1/connections/events/list": handleRequest(listConnectionEvents),
    "POST /api/v1/web_backend/describe_cron_expression": handleRequest(describeCronExpression),
  };
}

function assertValidScheduleUpdate(update: WebBackendConnectionUpdate) {
  if (update.scheduleType !== undefined) {
    expect(Object.values(ConnectionScheduleType)).toContain(update.scheduleType);
  }
  if (update.scheduleType === "cron") {
    expect(update.scheduleData?.cron?.cronExpression).toEqual(expect.any(String));
    expect(update.scheduleData?.cron?.cronTimeZone).toEqual(expect.any(String));
  }
  if (update.scheduleType === "basic") {
    expect(update.scheduleData?.basicSchedule?.timeUnit).toEqual(expect.any(String));
    expect(update.scheduleData?.basicSchedule?.units).toBeGreaterThan(0);
  }
}

function applyConnectionUpdate(saved: WebBackendConnectionRead, update: WebBackendConnectionUpdate) {
  const changes: Partial<WebBackendConnectionRead> = {
    name: update.name ?? saved.name,
    namespaceDefinition: update.namespaceDefinition ?? saved.namespaceDefinition,
    namespaceFormat: update.namespaceFormat ?? saved.namespaceFormat,
    prefix: update.prefix ?? saved.prefix,
    status: update.status ?? saved.status,
  };
  if (update.scheduleType !== undefined) {
    changes.scheduleType = update.scheduleType;
    changes.scheduleData = update.scheduleType === "manual" ? undefined : update.scheduleData;
  }

  Object.assign(saved, changes);
}
