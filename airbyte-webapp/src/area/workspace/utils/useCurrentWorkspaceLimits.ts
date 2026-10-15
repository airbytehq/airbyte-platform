import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useGetWorkspace } from "core/api";
import { Limit, WorkspaceRead } from "core/api/types/AirbyteClient";
import { useExperiment } from "core/services/Experiment";

interface CurrentWorkspaceLimits {
  activeConnectionLimitReached: boolean;
  sourceLimitReached: boolean;
  destinationLimitReached: boolean;
  limits?: WorkspaceRead["workspaceLimits"];
}

export const useCurrentWorkspaceLimits = (workspaceId?: string): CurrentWorkspaceLimits => {
  const currentWorkspaceId = useCurrentWorkspaceId();
  const workspace = useGetWorkspace(workspaceId ?? currentWorkspaceId);
  const showProductLimitsUI = useExperiment("productLimitsUI");

  if (!showProductLimitsUI || !workspace?.workspaceLimits) {
    return {
      activeConnectionLimitReached: false,
      sourceLimitReached: false,
      destinationLimitReached: false,
    };
  }

  return {
    activeConnectionLimitReached: isLimitReached(workspace.workspaceLimits.activeConnections),
    sourceLimitReached: isLimitReached(workspace.workspaceLimits.sources),
    destinationLimitReached: isLimitReached(workspace.workspaceLimits.destinations),
    limits: workspace.workspaceLimits,
  };
};

// a limit without a max is unlimited
const isLimitReached = (limit: Limit) => limit.max !== undefined && limit.current >= limit.max;
