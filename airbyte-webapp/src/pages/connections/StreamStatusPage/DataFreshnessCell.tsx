import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

export const DataFreshnessCell: React.FC<{ transitionedAt: number | undefined; showRelativeTime: boolean }> = ({
  transitionedAt,
  showRelativeTime,
}) => {
  const lastSyncDisplayText = useMemo(() => {
    if (transitionedAt) {
      const lastSync = dayjs(transitionedAt);

      if (showRelativeTime) {
        return lastSync.fromNow();
      }
      return lastSync.format("MM.DD.YY HH:mm:ss");
    }

    return <FormattedMessage id="general.dash" />;
  }, [transitionedAt, showRelativeTime]);

  if (lastSyncDisplayText) {
    return (
      <Text size="xs" color="grey300" data-testid="streams-list-data-freshness-cell-content">
        {lastSyncDisplayText}
      </Text>
    );
  }
  return null;
};
