import {
  Query,
  useInfiniteQuery,
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from "@tanstack/react-query";

import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "./agentsPlatform";
import { connectionsKeys, getNextConnectionListCursor } from "./connections";
import {
  createFusionConnectionEnablement,
  getFusionConnectionEnablement,
  getFusionSourceSearchStatus,
  requestFusionConnectionBackfill,
  updateFusionConnectionEnablement,
  webBackendListConnectionsForWorkspace,
} from "../generated/AirbyteClient";
import { FusionSearchStatusResponse, WebBackendConnectionReadList } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

const SEMANTIC_SEARCH_CONNECTIONS_PAGE_SIZE = 25;

export const connectionSemanticSearchKeys = {
  enablement: (connectionId: string) => [...connectionsKeys.all, "semanticSearchEnablement", connectionId] as const,
  sourceSearchStatuses: () => [...connectionsKeys.all, "semanticSearchSourceStatus"] as const,
  sourceSearchStatus: (sourceId: string) => [...connectionsKeys.all, "semanticSearchSourceStatus", sourceId] as const,
};

/**
 * Lists the connections in one workspace whose source and destination are both supported by the context layer.
 */
export const useSemanticSearchConnections = (workspaceId: string) => {
  const requestOptions = useRequestOptions();
  const sourceDefinitionIds = Array.from(useAgentsSupportedSourceDefinitionIds());
  const destinationDefinitionIds = Array.from(useAgentsSupportedDestinationDefinitionIds());

  return useInfiniteQuery(
    // Kept under the connection list keys so connection create, update, and delete invalidations refresh it.
    connectionsKeys.list(["semanticSearch", workspaceId]),
    async ({ pageParam: cursor }): Promise<WebBackendConnectionReadList> => {
      // The API ignores an empty definition filter, so an empty supported list would return every connection.
      if (sourceDefinitionIds.length === 0 || destinationDefinitionIds.length === 0) {
        return { connections: [] };
      }

      return webBackendListConnectionsForWorkspace(
        {
          workspaceId,
          cursor,
          pageSize: SEMANTIC_SEARCH_CONNECTIONS_PAGE_SIZE,
          filters: { sourceDefinitionIds, destinationDefinitionIds },
        },
        requestOptions
      );
    },
    {
      getNextPageParam: getNextConnectionListCursor,
      // One query runs per workspace. Reuse recent pages on remount, like the Agent access inventory queries.
      staleTime: 30_000,
    }
  );
};

export const useConnectionSemanticSearchEnablement = (connectionId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(
    connectionSemanticSearchKeys.enablement(connectionId),
    () => getFusionConnectionEnablement(connectionId, requestOptions),
    {
      retry: false,
      staleTime: 30_000,
    }
  );
};

/**
 * Turns semantic search on or off for one connection. The update is a compare-and-set: the server rejects it with
 * 409 when the saved value differs from expectedEnabled.
 */
export const useUpdateConnectionSemanticSearchEnablement = (connectionId: string) => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    ({ enabled, expectedEnabled }: { enabled: boolean; expectedEnabled: boolean }) =>
      updateFusionConnectionEnablement(
        connectionId,
        { enable_indexing: enabled, expectedState: { enable_indexing: expectedEnabled } },
        requestOptions
      ),
    {
      // Cancel a read that is still running, so its older value cannot overwrite the saved one.
      onMutate: () => queryClient.cancelQueries(connectionSemanticSearchKeys.enablement(connectionId)),
      onSuccess: (response) => {
        queryClient.setQueryData(connectionSemanticSearchKeys.enablement(connectionId), response);
        // The connection's backfill status changes with the setting, for example to off when the index is deleted.
        // The connection's source is not known here, so every loaded source status is read again.
        void queryClient.invalidateQueries(connectionSemanticSearchKeys.sourceSearchStatuses());
      },
      // After a failure the saved value can differ from the shown one: another change landed first (stale
      // expected state), or the server saved a turn-off but could not sync it to Agents. Read it again.
      onError: () => queryClient.invalidateQueries(connectionSemanticSearchKeys.enablement(connectionId)),
    }
  );
};

/**
 * Saves the first semantic search value of a new connection. The connection ID is a mutation variable, because the
 * caller creates the connection in the same flow. The server accepts the value only while the connection has no
 * saved value (off) or already has the same value. Otherwise it returns 409.
 */
export const useCreateConnectionSemanticSearchEnablement = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    ({ connectionId, enabled }: { connectionId: string; enabled: boolean }) =>
      createFusionConnectionEnablement(connectionId, { enable_indexing: enabled }, requestOptions),
    {
      onSuccess: async (response, { connectionId }) => {
        // Cancel a read that is still running, for example from the new connection's Settings tab, so its older
        // value cannot overwrite the saved one.
        await queryClient.cancelQueries(connectionSemanticSearchKeys.enablement(connectionId));
        queryClient.setQueryData(connectionSemanticSearchKeys.enablement(connectionId), response);
      },
    }
  );
};

interface SourceSearchStatusOptions {
  refetchInterval: (
    data: FusionSearchStatusResponse | undefined,
    query: Query<FusionSearchStatusResponse>
  ) => number | false;
}

/**
 * Reads the backfill and index status of each source. The response holds one entry for each indexed connection of the
 * source, so connections that share a source share one read. Call it only where semantic search is available: the
 * endpoint is Cloud only.
 *
 * Each read fails on its own and renders inline, so a failed read never breaks the other rows. A failed refetch keeps
 * the last data. The caller decides when to poll from the data and the query state.
 */
export const useSourceSearchStatuses = (sourceIds: string[], { refetchInterval }: SourceSearchStatusOptions) => {
  const requestOptions = useRequestOptions();

  return useQueries({
    queries: sourceIds.map(
      (sourceId): UseQueryOptions<FusionSearchStatusResponse> => ({
        queryKey: connectionSemanticSearchKeys.sourceSearchStatus(sourceId),
        queryFn: () => getFusionSourceSearchStatus(sourceId, requestOptions),
        retry: false,
        refetchInterval,
      })
    ),
  });
};

/**
 * Starts a backfill of every selected stream of one connection. An empty start time backfills the full history. The
 * server keeps a run that is still queued or running, so a retry is safe. After the request, the source status is read
 * again.
 */
export const useRequestConnectionBackfill = (connectionId: string, sourceId: string) => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    ({ startTime }: { startTime?: string }) =>
      requestFusionConnectionBackfill(connectionId, startTime ? { start_time: startTime } : {}, requestOptions),
    {
      // Not returned, so the caller gets the outcome without waiting for the new status.
      onSuccess: () => {
        void queryClient.invalidateQueries(connectionSemanticSearchKeys.sourceSearchStatus(sourceId));
      },
    }
  );
};
