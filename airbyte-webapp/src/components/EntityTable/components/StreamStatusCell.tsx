import { CellContext } from "@tanstack/react-table";
import classNames from "classnames";
import dayjs from "dayjs";

import { Tooltip } from "components/ui/Tooltip";

import {
  WebBackendConnectionRead,
  AirbyteStreamConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
  JobStatus,
  ConnectionStatus,
} from "core/request/AirbyteClient";
import { useGetConnection } from "hooks/services/useConnectionHook";

import styles from "./StreamStatusCell.module.scss";
import { Checkmark } from "./StreamStatusIcons/Checkmark";
import { Error } from "./StreamStatusIcons/Error";
import { Inactive } from "./StreamStatusIcons/Inactive";
import { Late } from "./StreamStatusIcons/Late";
import { Syncing } from "./StreamStatusIcons/Syncing";
import { AirbyteStreamAndConfiguration } from "../../../core/request/AirbyteClient";
import { ConnectionTableDataItem } from "../types";

// TOODO: Once available, add Error with Action
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

interface FakeStreamConfigWithStatus extends AirbyteStreamConfiguration {
  status: ConnectionStatus;
  latestSyncJobStatus?: JobStatus;
  scheduleType?: ConnectionScheduleType;
  latestSyncJobCreatedAt?: number;
  scheduleData?: ConnectionScheduleData;
  isSyncing: boolean;
}

interface AirbyteStreamWithStatusAndConfiguration extends AirbyteStreamAndConfiguration {
  config?: FakeStreamConfigWithStatus;
}

function filterStreamsWithTypecheck(
  v: AirbyteStreamWithStatusAndConfiguration | null
): v is AirbyteStreamWithStatusAndConfiguration {
  return Boolean(v);
}

const generateFakeStreamsWithStatus = (
  connection: WebBackendConnectionRead
): AirbyteStreamWithStatusAndConfiguration[] => {
  return connection.syncCatalog.streams
    .map<AirbyteStreamWithStatusAndConfiguration | null>(({ stream, config }) => {
      if (stream && config) {
        return {
          stream,
          config: {
            ...config,
            status: connection.status,
            latestSyncJobStatus: connection.latestSyncJobStatus,
            scheduleType: connection.scheduleType,
            latestSyncJobCreatedAt: connection.latestSyncJobCreatedAt,
            scheduleData: connection.scheduleData,
            isSyncing: connection.isSyncing || true,
          },
        };
      }
      return null;
    })
    .filter(filterStreamsWithTypecheck);
};

const getStatusForStream = (stream: AirbyteStreamWithStatusAndConfiguration): StatusType => {
  // connection.syncCatalog.streams[0].config?.selected
  if (stream.config && stream.config.selected) {
    if (stream.config.status === "active" && stream.config.latestSyncJobStatus !== "failed") {
      if (
        stream.config.scheduleType !== "manual" &&
        stream.config.latestSyncJobCreatedAt &&
        stream.config.scheduleData?.basicSchedule?.units &&
        // x1000 for a JS datetime
        stream.config.latestSyncJobCreatedAt * 1000 <
          dayjs()
            // Subtract 2x the scheduled interval and compare it to last sync time
            .subtract(stream.config.scheduleData.basicSchedule.units, stream.config.scheduleData.basicSchedule.timeUnit)
            .valueOf()
      ) {
        return "Behind";
      }
      return "On Track";
    } else if (stream.config.latestSyncJobStatus === "failed") {
      return "Error";
    }
  }
  return "Disabled";
};

const sortStreams = (streams: AirbyteStreamWithStatusAndConfiguration[]): Record<StatusType, number> =>
  streams.reduce(
    (sortedStreams, stream) => {
      sortedStreams[getStatusForStream(stream)]++;
      return sortedStreams;
    },
    { "On Track": 0, Behind: 0, Error: 0, Disabled: 0 }
  );

const StreamsBar: React.FC<{ streams: AirbyteStreamWithStatusAndConfiguration[] }> = ({ streams }) => {
  const sortedStreams = sortStreams(streams);
  return (
    <div className={styles.bar}>
      {(Object.entries(sortedStreams) as Array<[StatusType, number]>)
        .filter(([, count]) => !!count)
        .map(([statusType, count]) => (
          <div
            style={{ width: `${Number(count / streams.length) * 100}%` }}
            className={classNames(styles.filling, statusMap[statusType])}
            key={statusType}
          />
        ))}
    </div>
  );
};

const StreamsPerStatus: React.FC<{ streams: AirbyteStreamWithStatusAndConfiguration[] }> = ({ streams }) => {
  const sortedStreams = sortStreams(streams);
  return (
    <>
      {(Object.entries(sortedStreams) as Array<[StatusType, number]>)
        .filter(([, count]) => !!count)
        .map(([statusType, count], index) => (
          <div className={styles.tooltipContent} key={statusType}>
            <div className={styles.streamsDetail}>
              {iconMap[statusType]} <b>{count}</b> {statusType}
            </div>
            {streams[index].config?.isSyncing ? (
              <div className={styles.syncContainer}>
                {count} <Syncing className={styles.syncing} />
              </div>
            ) : null}
          </div>
        ))}
    </>
  );
};

const StreamStatusCellTooltipContent = ({ streams }: { streams: AirbyteStreamWithStatusAndConfiguration[] }) => {
  return (
    <div className={styles.tooltipContainer}>
      <StreamsBar streams={streams} />
      <StreamsPerStatus streams={streams} />
    </div>
  );
};

export const StreamsStatusCell: React.FC<CellContext<ConnectionTableDataItem, unknown>> = ({ row }) => {
  const connection = useGetConnection(row.original.connectionId);
  const fakeStreamsWithStatus = generateFakeStreamsWithStatus(connection);

  return (
    <Tooltip theme="light" control={<StreamsBar streams={fakeStreamsWithStatus} />}>
      <StreamStatusCellTooltipContent streams={fakeStreamsWithStatus} />
    </Tooltip>
  );
};
