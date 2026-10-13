import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "./agentsPlatform";
import { connectionsKeys, getNextConnectionListCursor } from "./connections";
import {
  getFusionConnectionEnablement,
  updateFusionConnectionEnablement,
  webBackendListConnectionsForWorkspace,
} from "../generated/AirbyteClient";
import { WebBackendConnectionReadList } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

const SEMANTIC_SEARCH_CONNECTIONS_PAGE_SIZE = 25;

export const connectionSemanticSearchKeys = {
  enablement: (connectionId: string) => [...connectionsKeys.all, "semanticSearchEnablement", connectionId] as const,
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
      },
      // After a failure the saved value can differ from the shown one: another change landed first (stale
      // expected state), or the server saved a turn-off but could not sync it to Agents. Read it again.
      onError: () => queryClient.invalidateQueries(connectionSemanticSearchKeys.enablement(connectionId)),
    }
  );
};
