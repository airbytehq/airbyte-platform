import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useGetWorkspace } from "core/api";
import { WorkspaceRead } from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

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

  if (!showProductLimitsUI || !workspace.workspaceLimits) {
    return {
      activeConnectionLimitReached: false,
      sourceLimitReached: false,
      destinationLimitReached: false,
    };
  }

  return {
    activeConnectionLimitReached:
      workspace.workspaceLimits.activeConnections.current >= workspace.workspaceLimits.activeConnections.max,
    sourceLimitReached: workspace.workspaceLimits.sources.current >= workspace.workspaceLimits.sources.max,
    destinationLimitReached:
      workspace.workspaceLimits?.destinations.current >= workspace.workspaceLimits?.destinations?.max,
    limits: workspace.workspaceLimits,
  };
};
