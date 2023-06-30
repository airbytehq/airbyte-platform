import React, { useMemo } from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { StatusIcon } from "components/ui/StatusIcon";
import { StatusIconStatus } from "components/ui/StatusIcon/StatusIcon";

import { EntityNameCell } from "./EntityNameCell";
import { Status } from "../types";

interface ConnectionStatusCellProps {
  status: string | null;
  value: string;
  enabled: boolean;
}

export const ConnectionStatusCell: React.FC<ConnectionStatusCellProps> = ({ status, value, enabled }) => {
  const { formatMessage } = useIntl();
  const isStreamCentricV2 = false;
  const statusIconStatus = useMemo<StatusIconStatus | undefined>(
    () =>
      status === Status.EMPTY
        ? "sleep"
        : status === Status.ACTIVE
        ? "success"
        : status === Status.INACTIVE
        ? "inactive"
        : status === Status.PENDING
        ? "loading"
        : status === Status.CANCELLED
        ? "cancelled"
        : undefined,
    [status]
  );
  const title =
    status === Status.EMPTY
      ? formatMessage({
          id: "connection.noSyncData",
        })
      : status === Status.INACTIVE
      ? formatMessage({
          id: "connection.disabledConnection",
        })
      : status === Status.ACTIVE
      ? formatMessage({
          id: "connection.successSync",
        })
      : status === Status.PENDING
      ? formatMessage({
          id: "connection.pendingSync",
        })
      : formatMessage({
          id: "connection.failedSync",
        });

  return (
    <FlexContainer alignItems="center">
      {!isStreamCentricV2 && <StatusIcon title={title} status={statusIconStatus} />}
      <EntityNameCell value={value} enabled={enabled} />
    </FlexContainer>
  );
};
