import { useEffect } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ExternalLink, Link } from "components/ui/Link";
import { Message } from "components/ui/Message";

import { useCurrentOrganizationId } from "area/organization/utils";
import { getOrganizationUsagePath } from "cloud/views/settings/routePaths";
import { useCurrentWorkspace, useGetConnectionStatusesCounts } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useLocalStorage } from "core/utils/useLocalStorage";

const getActionMessageId = ({
  isAllocationEnabled,
  canViewOrganizationUsage,
  isOnDemandCapacityEnabled,
}: {
  isAllocationEnabled: boolean;
  canViewOrganizationUsage: boolean;
  isOnDemandCapacityEnabled: boolean;
}) => {
  if (!isAllocationEnabled) {
    return isOnDemandCapacityEnabled ? "connection.capacityReached.onDemand" : undefined;
  }
  if (canViewOrganizationUsage) {
    return isOnDemandCapacityEnabled
      ? "connection.capacityReached.allocateOrOnDemand"
      : "connection.capacityReached.allocate";
  }
  return isOnDemandCapacityEnabled
    ? "connection.capacityReached.askAdminOrOnDemand"
    : "connection.capacityReached.askAdmin";
};

export const CapacityReachedMessage: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage, { organizationId });
  const hasDataWorkerCapacity = useFeature(FeatureItem.AllowDataWorkerCapacity);
  const isAllocationEnabled = useExperiment("platform.enable-data-worker-allocation") && hasDataWorkerCapacity;
  const isOnDemandCapacityEnabled = useFeature(FeatureItem.OnDemandCapacity);
  const { workspaceId } = useCurrentWorkspace();

  const [dismissedByWorkspace, setDismissedByWorkspace] = useLocalStorage(
    "airbyte_capacity-reached-banner-dismissed",
    {}
  );
  const { data: statusCounts } = useGetConnectionStatusesCounts();
  const queuedCount = statusCounts?.queued ?? 0;
  const isDataLoaded = statusCounts !== undefined;

  const isDismissed = dismissedByWorkspace[workspaceId] ?? false;

  // Clear the dismissed flag when the queue count becomes zero so the banner can reappear on the next queueing event
  useEffect(() => {
    if (isDataLoaded && queuedCount === 0 && isDismissed) {
      setDismissedByWorkspace((prev) => ({ ...prev, [workspaceId]: false }));
    }
  }, [isDataLoaded, queuedCount, isDismissed, workspaceId, setDismissedByWorkspace]);

  const showBanner = queuedCount > 0 && !isDismissed;

  const handleDismiss = () => {
    setDismissedByWorkspace((prev) => ({ ...prev, [workspaceId]: true }));
  };

  const actionMessageId = getActionMessageId({
    isAllocationEnabled,
    canViewOrganizationUsage,
    isOnDemandCapacityEnabled,
  });
  const usagePath = getOrganizationUsagePath(organizationId);

  if (!showBanner) {
    return null;
  }

  return (
    <Box px="xl" pb="xl">
      <Message
        type="warning"
        text={
          <>
            <FormattedMessage id="connection.capacityReached.banner" />
            {actionMessageId && (
              <>
                {" "}
                <FormattedMessage
                  id={actionMessageId}
                  values={{
                    allocate: (chunks) => <Link to={usagePath}>{chunks}</Link>,
                    onDemand: (chunks) => <ExternalLink href={links.dataWorkerOnDemandCapacity}>{chunks}</ExternalLink>,
                  }}
                />
              </>
            )}
          </>
        }
        onClose={handleDismiss}
        data-testid="capacity-reached-banner"
      />
    </Box>
  );
};
