import { useMemo } from "react";

import { useCurrentWorkspaceId } from "area/workspace/utils";

import { getExcludedConnectorIds } from "core/domain/connector/constants";
import { SourceDefinitionRead } from "core/request/AirbyteClient";
import { useSourceDefinitionList } from "services/connector/SourceDefinitionService";

/**
 * Returns a list of source definitions that are available for the current workspace.
 * The API alone will return too many definitions in the cloud context, and we need to filter
 * a few out with getExcludedConnectorIds()
 */
export const useAvailableSourceDefinitions = (): SourceDefinitionRead[] => {
  const workspaceId = useCurrentWorkspaceId();
  const { sourceDefinitions } = useSourceDefinitionList();

  return useMemo(() => {
    const excludedConnectorIds = getExcludedConnectorIds(workspaceId);
    return sourceDefinitions.filter(
      (sourceDefinition) => !excludedConnectorIds.includes(sourceDefinition.sourceDefinitionId)
    );
  }, [sourceDefinitions, workspaceId]);
};
