import { useMemo } from "react";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { getExcludedConnectorIds } from "core/domain/connector/constants";
import { DestinationDefinitionRead } from "core/request/AirbyteClient";
import { useDestinationDefinitionList } from "services/connector/DestinationDefinitionService";

/**
 * Returns a list of destination definitions that are available for the current workspace.
 * The API alone will return too many definitions in the cloud context, and we need to filter
 * a few out with getExcludedConnectorIds()
 */
export const useAvailableDestinationDefinitions = (): DestinationDefinitionRead[] => {
  const workspaceId = useCurrentWorkspaceId();
  const { destinationDefinitions } = useDestinationDefinitionList();

  return useMemo(() => {
    const excludedConnectorIds = getExcludedConnectorIds(workspaceId);
    return destinationDefinitions.filter(
      (destinationDefinition) => !excludedConnectorIds.includes(destinationDefinition.destinationDefinitionId)
    );
  }, [destinationDefinitions, workspaceId]);
};
