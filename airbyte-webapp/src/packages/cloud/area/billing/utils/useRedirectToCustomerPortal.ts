import { useCallback, useMemo, useState } from "react";
import { useIntl } from "react-intl";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useGetCustomerPortalUrl } from "core/api";
import { CustomerPortalRequestBodyFlow } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";

import { useLinkToBillingPage } from "./useLinkToBillingPage";

export const useRedirectToCustomerPortal = (flow: CustomerPortalRequestBodyFlow) => {
  const [redirecting, setRedirecting] = useState(false);
  const organizationId = useCurrentOrganizationId();
  const pathToBilling = useLinkToBillingPage();

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
