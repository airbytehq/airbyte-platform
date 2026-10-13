import { useInfiniteQuery } from "@tanstack/react-query";

import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "./agentsPlatform";
import { connectionsKeys, getNextConnectionListCursor } from "./connections";
import { webBackendListConnectionsForWorkspace } from "../generated/AirbyteClient";
import { WebBackendConnectionReadList } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

const SEMANTIC_SEARCH_CONNECTIONS_PAGE_SIZE = 25;

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
