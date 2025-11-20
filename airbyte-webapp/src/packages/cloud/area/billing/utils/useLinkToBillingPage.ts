import { useCurrentOrganizationId } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

/**
 * Returns the path to the billing page.
 */
export const useLinkToBillingPage = () => {
  const organizationId = useCurrentOrganizationId();
  return `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Billing}`;
};
