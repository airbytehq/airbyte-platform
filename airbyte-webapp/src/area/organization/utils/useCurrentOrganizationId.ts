import { useMatch } from "react-router-dom";

import { useCurrentWorkspaceOrUndefined, useFirstOrg } from "core/api";
import { RoutePaths } from "pages/routePaths";

export const useCurrentOrganizationId = () => {
  const workspace = useCurrentWorkspaceOrUndefined();
  const match = useMatch(`/${RoutePaths.Organization}/:organizationId/*`);
  const firstOrg = useFirstOrg();
  return workspace?.organizationId || match?.params.organizationId || firstOrg.organizationId;
};

export const useCurrentOrganizationIdFromUrl = () => {
  const match = useMatch(`/${RoutePaths.Organization}/:organizationId/*`);
  return match?.params.organizationId || "";
};
