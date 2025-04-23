import { useMatch, useSearchParams } from "react-router-dom";

import { WORKSPACE_ID_PARAM } from "pages/embedded/EmbeddedSourceCreatePage/hooks/useEmbeddedSourceParams";
import { RoutePaths } from "pages/routePaths";

export const useCurrentWorkspaceId = () => {
  const [searchParams] = useSearchParams();
  const match = useMatch(`/${RoutePaths.Workspaces}/:workspaceId/*`);
  return match?.params.workspaceId || searchParams.get(WORKSPACE_ID_PARAM) || "";
};
