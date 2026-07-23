import { useMatch } from "react-router-dom";

import { RoutePaths } from "pages/routePaths";

/**
 * Safe hook to optionally extract connection ID from the current URL.
 * Returns undefined if no connection ID is found in the URL, allowing
 * this hook to be used on any page without errors.
 *
 * Use this when connection context is optional (e.g., support widget).
 * For pages that require a connection ID, use `useCurrentConnectionId` instead.
 */
export const useCurrentConnectionIdOptional = (): string | undefined => {
  const match = useMatch(`/${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Connections}/:connectionId/*`);
  return match?.params.connectionId;
};
