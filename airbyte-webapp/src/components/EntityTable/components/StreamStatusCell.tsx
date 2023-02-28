import { CellContext } from "@tanstack/react-table";
import classNames from "classnames";
import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { Checkmark } from "components/icons/Checkmark";
import { Error } from "components/icons/Error";
import { Inactive } from "components/icons/Inactive";
import { Late } from "components/icons/Late";
import { Syncing } from "components/icons/Syncing";
import { Tooltip } from "components/ui/Tooltip";

import {
  WebBackendConnectionRead,
  AirbyteStreamConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
  JobStatus,
  ConnectionStatus,
} from "core/request/AirbyteClient";
import { AirbyteStreamAndConfiguration } from "core/request/AirbyteClient";
import { useGetConnection } from "hooks/services/useConnectionHook";

import styles from "./StreamStatusCell.module.scss";
import { ConnectionTableDataItem } from "../types";

const enum StreamStatusType {
  // TODO: When we have Actionable Errors, uncomment
  /* "actionRequired" = "actionRequired", */
  onTrack = "onTrack",
  disabled = "disabled",
  error = "error",
  behind = "behind",
}

const statusMap: Readonly<Record<StreamStatusType, string>> = {
  [StreamStatusType.onTrack]: styles.onTrack,
  [StreamStatusType.disabled]: styles.disabled,
  [StreamStatusType.error]: styles.error,
  [StreamStatusType.behind]: styles.behind,
};

const iconMap: Readonly<Record<StreamStatusType, React.ReactNode>> = {
  [StreamStatusType.onTrack]: <Checkmark />,
  [StreamStatusType.disabled]: <Inactive />,
  [StreamStatusType.error]: <Error />,
  [StreamStatusType.behind]: <Late />,
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

const isStreamBehind = (stream: AirbyteStreamWithStatusAndConfiguration) => {
  return (
    // This can be undefined due to historical data, but should always be present
    stream.config?.scheduleType &&
    !["cron", "manual"].includes(stream.config.scheduleType) &&
    stream.config.latestSyncJobCreatedAt &&
    stream.config.scheduleData?.basicSchedule?.units &&
    stream.config.latestSyncJobCreatedAt * 1000 < // x1000 for a JS datetime
      dayjs()
        // Subtract 2x the scheduled interval and compare it to last sync time
        .subtract(stream.config.scheduleData.basicSchedule.units, stream.config.scheduleData.basicSchedule.timeUnit)
        .valueOf()
  );
};

const getStatusForStream = (stream: AirbyteStreamWithStatusAndConfiguration): StreamStatusType => {
  if (stream.config && stream.config.selected) {
    if (stream.config.status === "active" && stream.config.latestSyncJobStatus !== "failed") {
      if (isStreamBehind(stream)) {
        return StreamStatusType.behind;
      }
      return StreamStatusType.onTrack;
    } else if (stream.config.latestSyncJobStatus === "failed") {
      return StreamStatusType.error;
    }
  }
  return StreamStatusType.disabled;
};

const sortStreams = (streams: AirbyteStreamWithStatusAndConfiguration[]): Record<StreamStatusType, number> =>
  streams.reduce(
    (sortedStreams, stream) => {
      sortedStreams[getStatusForStream(stream)]++;
      return sortedStreams;
    },
    // This is the intended display order thanks to Javascript object insertion order!
    {
      /* [StatusType.actionRequired]: 0, */ [StreamStatusType.error]: 0,
      [StreamStatusType.behind]: 0,
      [StreamStatusType.onTrack]: 0,
      [StreamStatusType.disabled]: 0,
    }
  );

const StreamsBar: React.FC<{ streams: AirbyteStreamWithStatusAndConfiguration[] }> = ({ streams }) => {
  const sortedStreams = sortStreams(streams);
  const nonEmptyStreams = Object.entries(sortedStreams).filter(([, count]) => !!count);
  return (
    <div className={styles.bar}>
      {nonEmptyStreams.map(([statusType, count]) => (
        <div
          style={{ width: `${Number(count / streams.length) * 100}%` }}
          className={classNames(styles.filling, statusMap[statusType as unknown as StreamStatusType])}
          key={statusType}
        />
      ))}
    </div>
  );
};

const StreamsPerStatus: React.FC<{ streams: AirbyteStreamWithStatusAndConfiguration[] }> = ({ streams }) => {
  const sortedStreams = sortStreams(streams);
  const nonEmptyStreams = Object.entries(sortedStreams).filter(([, count]) => !!count);
  return (
    <>
      {nonEmptyStreams.map(([statusType, count], index) => (
        <div className={styles.tooltipContent} key={statusType}>
          <div className={styles.streamsDetail}>
            {iconMap[statusType as unknown as StreamStatusType]} <b>{count}</b>{" "}
            <FormattedMessage id={`stream.status.${statusType}`} />
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

const StreamStatusPopover = ({ streams }: { streams: AirbyteStreamWithStatusAndConfiguration[] }) => {
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
      <StreamStatusPopover streams={fakeStreamsWithStatus} />
    </Tooltip>
  );
};
