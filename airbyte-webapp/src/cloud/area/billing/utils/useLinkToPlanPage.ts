import { useCurrentOrganizationId } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

/**
 * Returns the path to the plan page.
 */
export const useLinkToPlanPage = () => {
  const organizationId = useCurrentOrganizationId();
  return `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Plan}`;
};
