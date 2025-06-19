import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedDate } from "react-intl";

import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

export const DataFreshnessCell: React.FC<{ transitionedAt: number | undefined }> = ({ transitionedAt }) => {
  const relativeTime = useMemo(() => dayjs(transitionedAt).fromNow(), [transitionedAt]);

  if (relativeTime) {
    return (
      <Tooltip
        placement="top"
        control={
          <Text size="xs" color="grey300" data-testid="streams-list-data-freshness-cell-content">
            {relativeTime}
          </Text>
        }
      >
        <FormattedDate value={transitionedAt} timeStyle="long" dateStyle="medium" />
      </Tooltip>
    );
  }
  return null;
};
