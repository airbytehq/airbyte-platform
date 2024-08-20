import { Navigate } from "react-router-dom";

import { useCloudWorkspaceCount } from "core/api/cloud";

import { RoutePaths } from "../../../pages/routePaths";

export const CloudDefaultView: React.FC = () => {
  const count = useCloudWorkspaceCount();

  // Only show the workspace creation list if there is not exactly one workspace
  // otherwise redirect to the single workspace

  return (
    <Navigate
      to={
        count.count !== "one" ? `/${RoutePaths.Workspaces}` : `/${RoutePaths.Workspaces}/${count.workspace.workspaceId}`
      }
      replace
    />
  );
};

export default CloudDefaultView;
