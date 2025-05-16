import { useMatch } from "react-router-dom";

import { RoutePaths } from "pages/routePaths";

export const useCurrentOrganizationId = () => {
  const match = useMatch(`/${RoutePaths.Organization}/:organizationId/*`);
  return match?.params.organizationId || "";
};
