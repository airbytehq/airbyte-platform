import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { StatusIcon, StatusIconStatus } from "components/ui/StatusIcon";
import { Text } from "components/ui/Text";
interface AllConnectionsStatusCellProps {
  statuses?: Record<string, number>;
}

// The API just returns Record<string, number>, so we use this map to convert to supported statuses
const responseToStatusIconMap: Map<string, StatusIconStatus> = new Map([
  ["succeeded", "success"],
  ["failed", "error"],
]);

export const AllConnectionsStatusCell: React.FC<AllConnectionsStatusCellProps> = ({ statuses }) => {
  const statusesToDisplay: Array<{ status: StatusIconStatus; count: number }> = [];
  Object.entries(statuses || {}).forEach(([key, value]) => {
    const statusKey = responseToStatusIconMap.get(key);
    if (statusKey !== undefined && value > 0) {
      statusesToDisplay.push({
        status: statusKey,
        count: value,
      });
    }
  });

  return statusesToDisplay.length === 0 ? (
    <Text>
      <FormattedMessage id="general.dash" />
    </Text>
  ) : (
    <FlexContainer>
      {statusesToDisplay.map(({ status, count }) => (
        <StatusIcon status={status} value={count} size="sm" />
      ))}
    </FlexContainer>
  );
};
