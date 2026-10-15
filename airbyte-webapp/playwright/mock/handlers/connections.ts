import type { HandlerMap } from "./handler";
import type { Store, StoredConnection } from "../store";
import type {
  WebBackendConnectionListItem,
  WebBackendConnectionListRequestBody,
  WebBackendConnectionRead,
} from "@src/core/api/types/AirbyteClient";

import { notFound } from "./handler";
import { isOtherWorkspace } from "./identity";

/** A read of a deleted connection still answers, because the pages of a deleted connection need it. Lists hide it. */
function listedConnections(store: Store) {
  return store.connections.all().filter(({ status }) => status !== "deprecated");
}

/** The All Connections page always sends `filters`, with "", null, or [] for each filter that is not in use. */
function hasValue(value: unknown) {
  return value != null && value !== "" && !(Array.isArray(value) && value.length === 0);
}

/**
 * True when the request asks for a later page or a filtered list. The mock always returns every connection, so it
 * cannot answer these requests correctly.
 */
function asksForPageOrFilter({ cursor, sourceId, destinationId, filters }: WebBackendConnectionListRequestBody) {
  return Boolean(cursor) || [sourceId, destinationId, ...Object.values(filters ?? {})].some(hasValue);
}

function toListItem(connection: Readonly<WebBackendConnectionRead>): WebBackendConnectionListItem {
  const { source, destination } = connection;
  return {
    connectionId: connection.connectionId,
    name: connection.name,
    scheduleType: connection.scheduleType,
    scheduleData: connection.scheduleData,
    status: connection.status,
    source: {
      sourceId: source.sourceId,
      name: source.name,
      sourceDefinitionId: source.sourceDefinitionId,
      sourceName: source.sourceName,
    },
    destination: {
      destinationId: destination.destinationId,
      name: destination.name,
      destinationDefinitionId: destination.destinationDefinitionId,
      destinationName: destination.destinationName,
    },
    latestSyncJobCreatedAt: connection.latestSyncJobCreatedAt,
    latestSyncJobStatus: connection.latestSyncJobStatus,
    isSyncing: connection.isSyncing,
    schemaChange: connection.schemaChange,
    sourceActorDefinitionVersion: connection.sourceActorDefinitionVersion,
    destinationActorDefinitionVersion: connection.destinationActorDefinitionVersion,
    tags: connection.tags,
    onDemandEnabled: connection.onDemandEnabled,
  };
}

/** Connection reads, lists, updates, and deletes. The mock has no jobs, so every connection is idle. */
export const connectionHandlers: HandlerMap = {
  /**
   * With `withRefreshedCatalog`, the server discovers the source schema and diffs it against the connection. The mock
   * computes no diff: it answers with the catalog, diff and schemaChange that the test set with
   * `airbyte.schema.onRefresh`. The refresh does not change the stored connection.
   */
  webBackendGetConnection: ({ body, store }) => {
    const connection = store.connections.find(body.connectionId);
    if (!connection) {
      return notFound();
    }
    if (!body.withRefreshedCatalog) {
      return connection;
    }

    const refresh = store.schemaRefreshes.find(body.connectionId);
    if (!refresh) {
      throw new Error(
        `The webapp refreshed the schema of connection ${body.connectionId}, but the test set no answer. ` +
          "Call airbyte.schema.onRefresh() before the refresh."
      );
    }
    return {
      ...connection,
      syncCatalog: refresh.catalog,
      catalogDiff: refresh.diff,
      schemaChange: refresh.schemaChange,
    };
  },

  webBackendListConnectionsForWorkspace: ({ body, store }) => {
    if (isOtherWorkspace(store, body.workspaceId)) {
      return notFound();
    }
    if (asksForPageOrFilter(body)) {
      throw new Error("The mock does not support paging or filters on the connection list");
    }
    const connections = listedConnections(store);
    return { connections: connections.map(toListItem), page_size: body.pageSize, num_connections: connections.length };
  },

  webBackendGetConnectionStatusCounts: ({ body, store }) => {
    if (isOtherWorkspace(store, body.workspaceId)) {
      return notFound();
    }
    const connections = listedConnections(store);
    return {
      running: 0,
      queued: 0,
      healthy: 0,
      failed: 0,
      paused: connections.filter(({ status }) => status === "inactive").length,
      notSynced: connections.length,
    };
  },

  getConnectionStatuses: ({ body, store }) => {
    if (body.connectionIds.some((connectionId) => !store.connections.find(connectionId))) {
      return notFound();
    }
    return body.connectionIds.map((connectionId) => ({ connectionId, connectionSyncStatus: "pending" }));
  },

  listConnectionEventsMinimal: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : { events: [] },
  getStreamStatusesByRunState: ({ body, store }) =>
    store.connections.find(body.connectionId) ? { streamStatuses: [] } : notFound(),

  /**
   * Saves every field the webapp sent, so the next read shows the saved values. A null field means "no change", as
   * on the server. Not saved: `skipReset` and `sourceCatalogId`, which only control this request, and `operations`,
   * because the server adds IDs to operations when it saves them.
   */
  webBackendUpdateConnection: ({ body, store }) => {
    if (!store.connections.find(body.connectionId)) {
      return notFound();
    }
    const { connectionId, skipReset, sourceCatalogId, operations, ...fields } = body;
    const sent = Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== null));
    return store.connections.update(connectionId, sent as Partial<StoredConnection>);
  },

  /** The server does not remove a deleted connection. It sets the status to "deprecated" and answers 204. */
  deleteConnection: ({ body, store }) => {
    if (!store.connections.find(body.connectionId)) {
      return notFound();
    }
    store.connections.update(body.connectionId, { status: "deprecated" });
    return undefined;
  },
};
