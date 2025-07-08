import { useCallback } from "react";

import { RoutePaths } from "pages/routePaths";

/**
 * Returns a function that will take an absolute link (must begin with a slash)
 * and will return the full path within the workspace provided, e.g. the returned
 * function would turn `/connections/123` into `/workspaces/<workspace-id>/connections/123`
 */
export const useWorkspaceLink = (workSpaceId: string) => {
  return useCallback((link: string) => `/${RoutePaths.Workspaces}/${workSpaceId}${link}`, [workSpaceId]);
};
