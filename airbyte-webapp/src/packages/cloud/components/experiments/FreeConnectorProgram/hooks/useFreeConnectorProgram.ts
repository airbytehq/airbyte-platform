import { useState } from "react";
import { useIntl } from "react-intl";
import { useQuery } from "react-query";
import { useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { pollUntil } from "core/request/pollUntil";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { useNotificationService } from "hooks/services/Notification";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";

import { webBackendGetFreeConnectorProgramInfoForWorkspace } from "../lib/api";

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

  const enrollmentStatusQuery = useQuery(["freeConnectorProgramInfo", workspaceId], () =>
    webBackendGetFreeConnectorProgramInfoForWorkspace({ workspaceId }, requestOptions).then(
      ({ hasPaymentAccountSaved }) => {
        const userIsEligibleToEnroll = !hasPaymentAccountSaved;

        return {
          showEnrollmentUi: freeConnectorProgramEnabled && userIsEligibleToEnroll,
          isEnrolled: freeConnectorProgramEnabled && hasPaymentAccountSaved,
        };
      }
    )
  );

  // similar to the one above, but only returns eligible and non-eligible connection counts as this
  // is used separately in the app
  const connectionStatusQuery = useQuery(["freeConnectorProgramInfo", workspaceId], () =>
    webBackendGetFreeConnectorProgramInfoForWorkspace({ workspaceId }, requestOptions).then(
      ({ hasEligibleConnections, hasNonEligibleConnections }) => {
        return {
          hasEligibleConnections: freeConnectorProgramEnabled && hasEligibleConnections,
          hasNonEligibleConnections: freeConnectorProgramEnabled && hasNonEligibleConnections,
        };
      }
    )
  );

  return {
    enrollmentStatusQuery,
    userDidEnroll,
    connectionStatusQuery,
  };
};
