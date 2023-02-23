import { CellContext } from "@tanstack/react-table";
import classNames from "classnames";
import dayjs from "dayjs";

import { Tooltip } from "components/ui/Tooltip";

import { WebBackendConnectionRead } from "core/request/AirbyteClient";
import { useGetConnection } from "hooks/services/useConnectionHook";

import styles from "./StreamStatusCell.module.scss";
import { ConnectionTableDataItem } from "../types";

const statusMap = {
  active: styles.onTrack,
  disabled: styles.disabled,
  error: styles.error,
  behind: styles.behind,
};

const getStatusColor = (connection: WebBackendConnectionRead) => {
  if (connection.status === "active" && connection.latestSyncJobStatus !== "failed") {
    if (
      connection.scheduleType !== "manual" &&
      connection.latestSyncJobCreatedAt &&
      connection.scheduleData?.basicSchedule?.units &&
      connection.latestSyncJobCreatedAt <
        dayjs()
          .subtract(connection.scheduleData.basicSchedule.units, connection.scheduleData.basicSchedule.timeUnit)
          .valueOf() /
          1000 // Subtract 2x the scheduled interval and compare it to last sync time
    ) {
      return statusMap.behind;
    }
    return statusMap.active;
  } else if (connection.latestSyncJobStatus === "failed") {
    return statusMap.error;
  }
  return statusMap.disabled;
};

export const StreamsStatusCell: React.FC<CellContext<ConnectionTableDataItem, unknown>> = ({ row }) => {
  const connection = useGetConnection(row.original.connectionId);
  const filling = classNames(styles.filling, getStatusColor(connection));
  return (
    <Tooltip
      control={
        <div className={styles.container}>
          <div className={filling} />
        </div>
      }
    >
      <div className={styles.container} style={{ width: "100px" }}>
        <div className={filling} />
      </div>
    </Tooltip>
  );
  // return <>{connection.syncCatalog.streams.length}</>;
};
