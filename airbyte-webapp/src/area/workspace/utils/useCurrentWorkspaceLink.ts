import { useCallback } from "react";

import { useCurrentWorkspace } from "core/api";
import { RoutePaths } from "pages/routePaths";

/**
 * Returns a function that will take an absolute link (must begin with a slash)
 * and will return the full path within the current workspace, e.g. the returned
 * function would turn `/connections/123` into `/workspaces/<current-workspace-id>/connections/123`
 */
export const useCurrentWorkspaceLink = () => {
  const { workspaceId } = useCurrentWorkspace();

  return useCallback((link: string) => `/${RoutePaths.Workspaces}/${workspaceId}${link}`, [workspaceId]);
};
