import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { useExperiment } from "hooks/services/Experiment";

export const DataFreshnessCell: React.FC<{ transitionedAt: number | undefined; showRelativeTime: boolean }> = ({
  transitionedAt,
  showRelativeTime,
}) => {
  const showSyncProgress = useExperiment("connection.syncProgress", false);

  const lastSyncDisplayText = useMemo(() => {
    if (transitionedAt) {
      const lastSync = dayjs(transitionedAt);

      if (showRelativeTime) {
        return lastSync.fromNow();
      }
      return lastSync.format("MM.DD.YY HH:mm:ss");
    }

    return showSyncProgress ? <FormattedMessage id="general.dash" /> : null;
  }, [transitionedAt, showSyncProgress, showRelativeTime]);

  if (lastSyncDisplayText) {
    return (
      <Text size="xs" color="grey300">
        {lastSyncDisplayText}
      </Text>
    );
  }
  return null;
};
