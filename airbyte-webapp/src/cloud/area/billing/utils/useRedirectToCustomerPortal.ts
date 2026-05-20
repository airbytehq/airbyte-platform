import { useCallback, useMemo, useState } from "react";
import { useIntl } from "react-intl";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useGetCustomerPortalUrl } from "core/api";
import { CustomerPortalRequestBodyFlow, CustomerPortalRequestBodyPlan } from "core/api/types/AirbyteClient";
import { useExperiment } from "core/services/Experiment";
import { useNotificationService } from "core/services/Notification";
import { trackError } from "core/utils/datadog";

import { useLinkToBillingPage } from "./useLinkToBillingPage";
import { useLinkToPlanPage } from "./useLinkToPlanPage";

export const useRedirectToCustomerPortal = (
  flow: CustomerPortalRequestBodyFlow,
  plan?: CustomerPortalRequestBodyPlan
) => {
  const [redirecting, setRedirecting] = useState(false);
  const organizationId = useCurrentOrganizationId();
  const pathToBilling = useLinkToBillingPage();
  const pathToPlan = useLinkToPlanPage();
  const isSelfServePlusPlanEnabled = useExperiment("billing.selfServePlusPlan");
  const returnPath = flow === "setup" && isSelfServePlusPlanEnabled ? pathToPlan : pathToBilling;

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
        plan,
        organizationId,
        returnUrl: `${window.location.origin}${returnPath}`,
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
    plan,
    formatMessage,
    getCustomerPortalUrl,
    organizationId,
    returnPath,
    registerNotification,
    unregisterNotificationById,
  ]);

  const loadingURLOrRedirecting = useMemo(
    () => redirecting || isCustomerPortalUrlLoading,
    [redirecting, isCustomerPortalUrlLoading]
  );

  return { goToCustomerPortal, redirecting: loadingURLOrRedirecting };
};
