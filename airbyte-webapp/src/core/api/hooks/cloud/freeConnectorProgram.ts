import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { FeatureItem, useFeature } from "core/services/features";
import { pollUntil } from "core/utils/pollUntil";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";

import { webBackendGetFreeConnectorProgramInfoForWorkspace } from "../../generated/CloudApi";

export const STRIPE_SUCCESS_QUERY = "fcpEnrollmentSuccess";

export const useFreeConnectorProgram = () => {
  const workspaceId = useCurrentWorkspaceId();
  const middlewares = useDefaultRequestMiddlewares();
  const requestOptions = { middlewares };
  const freeConnectorProgramEnabled = useFeature(FeatureItem.FreeConnectorProgram);
  const [searchParams, setSearchParams] = useSearchParams();
  const [userDidEnroll, setUserDidEnroll] = useState(false);
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();

  const removeStripeSuccessQuery = () => {
    const { [STRIPE_SUCCESS_QUERY]: _, ...unrelatedSearchParams } = Object.fromEntries(searchParams);
    setSearchParams(unrelatedSearchParams, { replace: true });
  };

  useEffectOnce(() => {
    if (searchParams.has(STRIPE_SUCCESS_QUERY)) {
      pollUntil(
        () => webBackendGetFreeConnectorProgramInfoForWorkspace({ workspaceId }, requestOptions),
        ({ hasPaymentAccountSaved }) => hasPaymentAccountSaved,
        { intervalMs: 1000, maxTimeoutMs: 10000 }
      ).then((maybeFcpInfo) => {
        if (maybeFcpInfo) {
          removeStripeSuccessQuery();
          setUserDidEnroll(true);
          registerNotification({
            id: "fcp/enrollment-success",
            text: formatMessage({ id: "freeConnectorProgram.enroll.success" }),
            type: "success",
          });
        } else {
          trackError(new Error("Unable to confirm Free Connector Program enrollment before timeout"), { workspaceId });
          registerNotification({
            id: "fcp/enrollment-failure",
            text: formatMessage({ id: "freeConnectorProgram.enroll.failure" }),
            type: "error",
          });
        }
      });
    }
  });

  const programStatusQuery = useQuery(["freeConnectorProgramInfo", workspaceId], () =>
    webBackendGetFreeConnectorProgramInfoForWorkspace({ workspaceId }, requestOptions).then(
      ({ hasPaymentAccountSaved, hasEligibleConnections, hasNonEligibleConnections }) => {
        const userIsEligibleToEnroll = !hasPaymentAccountSaved;

        return {
          showEnrollmentUi: freeConnectorProgramEnabled && userIsEligibleToEnroll,
          isEnrolled: freeConnectorProgramEnabled && hasPaymentAccountSaved,
          hasEligibleConnections: freeConnectorProgramEnabled && hasEligibleConnections,
          hasNonEligibleConnections: freeConnectorProgramEnabled && hasNonEligibleConnections,
        };
      }
    )
  );

  return {
    programStatusQuery,
    userDidEnroll,
  };
};
