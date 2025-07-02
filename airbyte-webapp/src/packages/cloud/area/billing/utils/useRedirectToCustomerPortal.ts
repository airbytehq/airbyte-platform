import { useCallback, useMemo, useState } from "react";
import { useIntl } from "react-intl";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useCurrentWorkspaceOrUndefined, useGetCustomerPortalUrl } from "core/api";
import { CustomerPortalRequestBodyFlow } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

export const useRedirectToCustomerPortal = (flow: CustomerPortalRequestBodyFlow) => {
  const [redirecting, setRedirecting] = useState(false);
  const workspace = useCurrentWorkspaceOrUndefined();
  const createWorkspaceLink = useCurrentWorkspaceLink();
  const organizationId = useCurrentOrganizationId();

  // NOTE: this is transitional code to support the transition to multi-org.
  // Once multi-org is fully launched, we should remove the workspace check and
  // just use the organizationId.
  const pathToBilling = workspace
    ? createWorkspaceLink(`/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Billing}`)
    : `/${RoutePaths.Organization}/${organizationId}/${CloudSettingsRoutePaths.Billing}`;

  const { mutateAsync: getCustomerPortalUrl, isLoading: isCustomerPortalUrlLoading } = useGetCustomerPortalUrl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { formatMessage } = useIntl();

  const goToCustomerPortal = useCallback(async () => {
    setRedirecting(true);
    const CUSTOMER_PORTAL_NOTIFICATION_ID = "getCustomerPortalUrl";
    unregisterNotificationById(CUSTOMER_PORTAL_NOTIFICATION_ID);
    try {
      const { url } = await getCustomerPortalUrl({
        flow,
        organizationId,
        returnUrl: `${window.location.origin}${pathToBilling}`,
      });
      window.location.assign(url);
    } catch (e) {
      setRedirecting(false);
      trackError(e);
      registerNotification({
        id: "getCustomerPortalUrl",
        type: "error",
        text: formatMessage({ id: "settings.organization.billing.customerPortalURLFailure" }),
      });
    }
  }, [
    flow,
    formatMessage,
    getCustomerPortalUrl,
    organizationId,
    pathToBilling,
    registerNotification,
    unregisterNotificationById,
  ]);

  const loadingURLOrRedirecting = useMemo(
    () => redirecting || isCustomerPortalUrlLoading,
    [redirecting, isCustomerPortalUrlLoading]
  );

  return { goToCustomerPortal, redirecting: loadingURLOrRedirecting };
};
