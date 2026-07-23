import { useEffect } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Message } from "components/ui/Message";

import { useCurrentWorkspace, useGetConnectionStatusesCounts } from "core/api";
import { useLocalStorage } from "core/utils/useLocalStorage";

export const CapacityReachedMessage: React.FC = () => {
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

  if (!showBanner) {
    return null;
  }

  return (
    <Box px="xl" pb="xl">
      <Message
        type="warning"
        text={<FormattedMessage id="connection.capacityReached.banner" />}
        onClose={handleDismiss}
        data-testid="capacity-reached-banner"
      />
    </Box>
  );
};
