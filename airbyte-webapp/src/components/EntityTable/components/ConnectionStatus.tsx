import React, { useMemo } from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { StatusIcon } from "components/ui/StatusIcon";
import { StatusIconStatus } from "components/ui/StatusIcon/StatusIcon";

import { useExperiment } from "hooks/services/Experiment";

import { EntityNameCell } from "./EntityNameCell";
import { StreamsStatusCell } from "./StreamStatusCell";
import { Status } from "../types";

interface ConnectionStatusProps {
  connectionId: string;
  status: string | null;
  value: string;
  enabled: boolean;
}

const ConnectionStatusPrev: React.FC<Pick<ConnectionStatusProps, "status">> = ({ status }) => {
  const { formatMessage } = useIntl();
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
      : status === Status.CANCELLED
      ? formatMessage({ id: "connection.cancelledSync" })
      : formatMessage({
          id: "connection.failedSync",
        });

  return <StatusIcon title={title} status={statusIconStatus} />;
};

export const ConnectionStatus: React.FC<ConnectionStatusProps> = ({ connectionId, status, value, enabled }) => {
  const isAllConnectionsStatusEnabled = useExperiment("connections.connectionsStatusesEnabled");

  return (
    <FlexContainer alignItems="center">
      {isAllConnectionsStatusEnabled ? (
        <StreamsStatusCell connectionId={connectionId} />
      ) : (
        <ConnectionStatusPrev status={status} />
      )}
      <EntityNameCell value={value} enabled={enabled} />
    </FlexContainer>
  );
};
