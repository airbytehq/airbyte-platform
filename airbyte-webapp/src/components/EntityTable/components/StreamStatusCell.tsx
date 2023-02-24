import { CellContext } from "@tanstack/react-table";
import classNames from "classnames";
import dayjs from "dayjs";

import { Tooltip } from "components/ui/Tooltip";

import { WebBackendConnectionRead } from "core/request/AirbyteClient";
import { useGetConnection } from "hooks/services/useConnectionHook";

import styles from "./StreamStatusCell.module.scss";
import { Checkmark } from "./StreamStatusIcons/Checkmark";
import { Error } from "./StreamStatusIcons/Error";
import { Inactive } from "./StreamStatusIcons/Inactive";
import { Late } from "./StreamStatusIcons/Late";
import { Syncing } from "./StreamStatusIcons/Syncing";
import { ConnectionTableDataItem } from "../types";

type StatusType = "On Track" | "Disabled" | "Error" | "Behind";

const statusMap: Readonly<Record<StatusType, string>> = {
  "On Track": styles.onTrack,
  Disabled: styles.disabled,
  Error: styles.error,
  Behind: styles.behind,
};

const iconMap: Readonly<Record<StatusType, React.ReactNode>> = {
  "On Track": <Checkmark />,
  Disabled: <Inactive />,
  Error: <Error />,
  Behind: <Late />,
};

const getStatusType = (connection: WebBackendConnectionRead): StatusType => {
  // connection.syncCatalog.streams[0].config?.selected
  if (connection.status === "active" && connection.latestSyncJobStatus !== "failed") {
    if (
      connection.scheduleType !== "manual" &&
      connection.latestSyncJobCreatedAt &&
      connection.scheduleData?.basicSchedule?.units &&
      // x1000 for a JS datetime
      connection.latestSyncJobCreatedAt * 1000 <
        dayjs()
          // Subtract 2x the scheduled interval and compare it to last sync time
          .subtract(connection.scheduleData.basicSchedule.units, connection.scheduleData.basicSchedule.timeUnit)
          .valueOf()
    ) {
      return "Behind";
    }
    return "On Track";
  } else if (connection.latestSyncJobStatus === "failed") {
    return "Error";
  }
  return "Disabled";
};

const StreamStatusCellTooltipContent = ({
  connection,
  statusType,
}: {
  connection: WebBackendConnectionRead;
  statusType: StatusType;
}) => {
  const filling = classNames(styles.filling, statusMap[statusType]);
  return (
    <div className={styles.tooltipContainer}>
      <div className={styles.bar}>
        <div className={filling} />
      </div>
      <div className={styles.tooltipContent}>
        <div className={styles.streamsDetail}>
          {iconMap[statusType]} <b>{connection.syncCatalog.streams.length}</b> {statusType}
        </div>
        {connection.isSyncing ? (
          <div className={styles.syncContainer}>
            <Syncing className={styles.syncing} /> {connection.syncCatalog.streams.length}
          </div>
        ) : null}
      </div>
    </div>
  );
};

export const StreamsStatusCell: React.FC<CellContext<ConnectionTableDataItem, unknown>> = ({ row }) => {
  const connection = useGetConnection(row.original.connectionId);
  const statusType = getStatusType(connection);
  const filling = classNames(styles.filling, statusMap[statusType]);
  return (
    <Tooltip
      theme="light"
      control={
        <div className={styles.bar}>
          <div className={filling} />
        </div>
      }
    >
      <StreamStatusCellTooltipContent connection={connection} statusType={statusType} />
    </Tooltip>
  );
};
