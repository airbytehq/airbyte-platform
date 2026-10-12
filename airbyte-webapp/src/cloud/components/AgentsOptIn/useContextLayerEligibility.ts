import { useAgentsProvisioningStatusQuery } from "core/api";
import { useIsCloudApp } from "core/utils/app";

import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

export const useContextLayerEligibility = () => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const statusQuery = useAgentsProvisioningStatusQuery({ enabled: isCloudApp });
  const status = statusQuery.data;
  const isEligible = Boolean(status && (status.is_enrolled || (status.external_cloud_eligible && showAgentsOptIn)));

  return { isCloudApp, statusQuery, status, isEligible };
};
