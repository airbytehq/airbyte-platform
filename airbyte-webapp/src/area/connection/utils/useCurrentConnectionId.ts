import { useMatch } from "react-router-dom";

import { RoutePaths } from "pages/routePaths";

export const useCurrentConnectionId = () => {
  const match = useMatch(`/${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Connections}/:connectionId/*`);

  if (!match?.params.connectionId) {
    throw new Error("No connectionId found in the current URL");
  }

  return match?.params.connectionId;
};
