import { useMatch } from "react-router-dom";

import { RoutePaths } from "pages/routePaths";

export const useCurrentWorkspaceId = () => {
  const match = useMatch(`/${RoutePaths.Workspaces}/:workspaceId/*`);
  return match?.params.workspaceId || "";
};
