import { useGetCloudWorkspaceUsage } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

export const useCreditsUsage = () => {
  const { workspaceId } = useCurrentWorkspace();
  const data = useGetCloudWorkspaceUsage(workspaceId);

  return { data };
};
