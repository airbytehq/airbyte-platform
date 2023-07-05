import { useCallback } from "react";
import { useNavigate } from "react-router-dom";

import { RoutePaths } from "pages/routePaths";

export const useSelectWorkspace = (): ((workspace?: string | null) => void) => {
  const navigate = useNavigate();

  return useCallback(
    (workspace) => {
      navigate(`/${RoutePaths.Workspaces}/${workspace ?? ""}`);
    },
    [navigate]
  );
};
