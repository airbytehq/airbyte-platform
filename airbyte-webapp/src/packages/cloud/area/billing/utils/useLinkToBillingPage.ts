import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useExperiment } from "hooks/services/Experiment";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

/**
 * Returns the path to the billing page.
 *
 * While we are rolling out the new organization-level routes, there are two possible links to billing:
 * 1. If the experiment is false: /workspaces/<current-workspace-id>/settings/billing
 * 2. If the experiment is true: /organizations/<current-organization-id>/settings/billing
 *
 * The first case can be removed entirely once we roll out the new organization pages:
 * https://github.com/airbytehq/airbyte-internal-issues/issues/13025
 */
export const useLinkToBillingPage = () => {
  const showOrgPicker = useExperiment("sidebar.showOrgPicker");
  const workspaceId = useCurrentWorkspaceId();
  const organizationId = useCurrentOrganizationId();
  return showOrgPicker
    ? `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Billing}`
    : `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Billing}`;
};
