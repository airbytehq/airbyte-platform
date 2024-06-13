import React, { useMemo } from "react";
import { useIntl } from "react-intl";

import { StatusIcon, StatusIconProps } from "components/ui/StatusIcon";
import { StatusIconStatus } from "components/ui/StatusIcon/StatusIcon";

import { Status } from "../types";

const _statusConfig: Array<{ status: Status; statusIconStatus?: StatusIconStatus; titleId: string }> = [
  { status: Status.ACTIVE, statusIconStatus: "success", titleId: "connection.successSync" },
  { status: Status.INACTIVE, statusIconStatus: "inactive", titleId: "connection.disabledConnection" },
  { status: Status.FAILED, statusIconStatus: "error", titleId: "connection.failedSync" },
  { status: Status.EMPTY, statusIconStatus: "sleep", titleId: "connection.noSyncData" },
];

interface AllConnectionStatusConnectEntity {
  name: string;
  connector: string;
  status: string;
  lastSyncStatus: string | null;
}

interface AllConnectionsStatusCellProps {
  connectEntities: AllConnectionStatusConnectEntity[];
}

const AllConnectionsStatusCell: React.FC<AllConnectionsStatusCellProps> = ({ connectEntities }) => {
  const { formatMessage } = useIntl();

  const propsForStatusIcons: StatusIconProps[] = useMemo(() => {
    const allIconProps: StatusIconProps[] = [];
    for (const { status, statusIconStatus, titleId } of _statusConfig) {
      const filteredEntities = connectEntities.filter((entity) => entity.lastSyncStatus === status);
      if (filteredEntities.length) {
        allIconProps.push({
          status: statusIconStatus,
          value: filteredEntities.length,
          title: titleId,
        });
      }
    }

    return allIconProps;
  }, [connectEntities]);

  return (
    <>
      {propsForStatusIcons.map((statusIconProps) => (
        <StatusIcon
          key={statusIconProps.title}
          {...statusIconProps}
          title={formatMessage({ id: statusIconProps.title })}
          size="sm"
        />
      ))}
    </>
  );
};

export default AllConnectionsStatusCell;
