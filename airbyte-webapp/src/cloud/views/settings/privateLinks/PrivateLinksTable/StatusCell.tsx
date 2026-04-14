import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { PrivateLinkStatus } from "core/api/types/AirbyteClient";

const IN_PROGRESS_STATUSES: ReadonlySet<PrivateLinkStatus> = new Set([
  PrivateLinkStatus.creating,
  PrivateLinkStatus.configuring,
  PrivateLinkStatus.deleting,
]);

interface StatusCellProps {
  status: PrivateLinkStatus;
}

export const StatusCell: React.FC<StatusCellProps> = ({ status }) => {
  const { formatMessage } = useIntl();
  const isInProgress = IN_PROGRESS_STATUSES.has(status);

  return (
    <FlexContainer alignItems="center" gap="sm">
      {isInProgress && <Spinner size="xs" />}
      <Text size="sm">{formatMessage({ id: `settings.privateLinks.status.${status}` })}</Text>
    </FlexContainer>
  );
};
