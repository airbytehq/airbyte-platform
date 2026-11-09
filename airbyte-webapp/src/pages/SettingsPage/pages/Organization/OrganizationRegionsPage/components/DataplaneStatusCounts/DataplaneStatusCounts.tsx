import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { HEALTH_STATUSES, healthStatusIcon, healthStatusSummaryMessageId } from "../../healthStatus";
import { Region } from "../../types";

interface DataplaneStatusCountsProps {
  region: Region;
}

export const DataplaneStatusCounts: React.FC<DataplaneStatusCountsProps> = ({ region }) => {
  const { formatMessage } = useIntl();
  return (
    <FlexContainer alignItems="center" gap="md" data-testid="region-status-counts">
      {HEALTH_STATUSES.map((status) => {
        const count = region.dataplanes.filter((dataplane) => dataplane.status === status).length;
        if (count === 0) {
          return null;
        }
        const icon = healthStatusIcon[status];
        return (
          <FlexContainer
            key={status}
            alignItems="center"
            gap="xs"
            title={formatMessage({ id: healthStatusSummaryMessageId[status] }, { count })}
          >
            <Icon type={icon.type} color={icon.color} size="sm" />
            <Text size="md">{count}</Text>
          </FlexContainer>
        );
      })}
    </FlexContainer>
  );
};
