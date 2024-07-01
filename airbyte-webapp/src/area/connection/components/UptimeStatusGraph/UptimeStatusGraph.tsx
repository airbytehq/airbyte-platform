import dayjs from "dayjs";
import React, { ComponentPropsWithoutRef, useEffect, useMemo, useState } from "react";
import { ResponsiveContainer, Tooltip, XAxis } from "recharts";
// these are not worth typing
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore-next-line
import { generateCategoricalChart } from "recharts/es6/chart/generateCategoricalChart";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore-next-line
import { formatAxisMap } from "recharts/es6/util/CartesianUtils";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";

import { getStreamKey } from "area/connection/utils";
import { useGetConnectionSyncProgress, useGetConnectionUptimeHistory } from "core/api";
import { ConnectionSyncProgressRead, ConnectionUptimeHistoryRead, JobStatus } from "core/api/types/AirbyteClient";
import { assertNever } from "core/utils/asserts";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import { UpdateTooltipTickPositions } from "./UpdateTooltipTickPositions";
import styles from "./UptimeStatusGraph.module.scss";
import { UptimeStatusGraphTooltip } from "./UptimeStatusGraphTooltip";
import { ChartStream, UptimeDayEntry, Waffle } from "./WaffleChart";
import {
  CHART_BASE_HEIGHT,
  CHART_MAX_HEIGHT,
  CHART_MIN_HEIGHT,
  CHART_STREAM_ROW_HEIGHT,
} from "../DataMovedGraph/constants";
import { ClickToJob, tooltipConfig, xAxisConfig } from "../HistoricalOverview/ChartConfig";
import { NoDataMessage } from "../HistoricalOverview/NoDataMessage";

const StreamChart = generateCategoricalChart({
  chartName: "StreamChart",
  GraphicalChild: [Waffle],
  axisComponents: [{ axisType: "xAxis", AxisComp: XAxis }],
  formatAxisMap,
});

const generatePlaceholderHistory = (
  connectionSyncProgress?: ConnectionSyncProgressRead
): ConnectionUptimeHistoryRead => {
  if (
    !connectionSyncProgress ||
    connectionSyncProgress.configType === "clear" ||
    connectionSyncProgress.configType === "reset_connection"
  ) {
    return [];
  }

  return [
    {
      bytesCommitted: connectionSyncProgress.bytesCommitted ?? 0,
      bytesEmitted: connectionSyncProgress.bytesCommitted ?? 0,
      configType: connectionSyncProgress.configType,
      jobId: connectionSyncProgress.jobId ?? 0,
      jobCreatedAt: connectionSyncProgress.syncStartedAt ?? dayjs().unix(),
      jobUpdatedAt: dayjs().unix(),
      recordsCommitted: connectionSyncProgress.recordsCommitted ?? 0,
      recordsEmitted: connectionSyncProgress.recordsEmitted ?? 0,
      streamStatuses: connectionSyncProgress.streams.map((syncProgressItem) => {
        return {
          status: "running",
          streamName: syncProgressItem.streamName,
          streamNamespace: syncProgressItem.streamNamespace ?? "",
        };
      }),
    },
  ];
};

type SortableStream = Pick<ChartStream, "streamName"> & Partial<Pick<ChartStream, "streamNamespace" | "status">>;

const statusOrder: ConnectionStatusIndicatorStatus[] = [
  ConnectionStatusIndicatorStatus.Disabled,
  ConnectionStatusIndicatorStatus.Pending,
  ConnectionStatusIndicatorStatus.OnTime,
  ConnectionStatusIndicatorStatus.OnTrack,
  ConnectionStatusIndicatorStatus.Late,
  ConnectionStatusIndicatorStatus.Error,
  ConnectionStatusIndicatorStatus.ActionRequired,
];

const sortStreams = (a: SortableStream, b: SortableStream) => {
  const { streamName: nameA, streamNamespace: namespaceA, status: statusA } = a;
  const { streamName: nameB, streamNamespace: namespaceB, status: statusB } = b;

  if (statusA && statusB) {
    const statusCompare = Math.sign(statusOrder.indexOf(statusA) - statusOrder.indexOf(statusB));
    if (statusCompare !== 0) {
      return statusCompare;
    }
  }

  const namespaceCompare = (namespaceA ?? "")?.localeCompare(namespaceB ?? "");
  if (namespaceCompare !== 0) {
    return namespaceCompare;
  }

  return nameA.localeCompare(nameB);
};

interface RunBucket {
  jobId: number;
  runtimeMs: number;
  recordsEmitted: number;
  recordsCommitted: number;
  streams: ChartStream[];
}

const formatDataForChart = (data: ReturnType<typeof useGetConnectionUptimeHistory>) => {
  // bucket entries by their timestamp and collect all stream identities so we can fill in gaps
  const dateBuckets: Record<string, RunBucket> = {};

  const { bucketedEntries, allStreamIdentities } = data.reduce<{
    bucketedEntries: typeof dateBuckets;
    allStreamIdentities: Map<string, SortableStream>;
  }>(
    (acc, entry) => {
      // not destructuring to avoid creating new objects when returning
      const bucketedEntries = acc.bucketedEntries;
      const allStreamIdentities = acc.allStreamIdentities;

      // bucket this entry's statuses
      bucketedEntries[entry.jobCreatedAt] = {
        jobId: entry.jobId,
        runtimeMs: (entry.jobUpdatedAt - entry.jobCreatedAt) * 1000,
        recordsEmitted: entry.recordsEmitted,
        recordsCommitted: entry.recordsCommitted,
        streams: [],
      };
      entry.streamStatuses.reduce<{ bucket: RunBucket; allStreamIdentities: typeof allStreamIdentities }>(
        ({ bucket, allStreamIdentities }, streamStatus) => {
          let status: ConnectionStatusIndicatorStatus = ConnectionStatusIndicatorStatus.Pending;

          switch (streamStatus.status) {
            case JobStatus.succeeded:
              status = ConnectionStatusIndicatorStatus.OnTime;
              break;
            case JobStatus.failed:
              status = ConnectionStatusIndicatorStatus.Error;
              break;
            case JobStatus.running:
              status = ConnectionStatusIndicatorStatus.Syncing;
              break;
            case JobStatus.cancelled:
            case JobStatus.incomplete:
            case JobStatus.pending:
              status = ConnectionStatusIndicatorStatus.Pending;
              break;
            default:
              assertNever(streamStatus.status);
          }

          bucket.streams.push({
            streamNamespace: streamStatus.streamNamespace,
            streamName: streamStatus.streamName,
            status,
          });

          // add this stream to the map
          const streamKey = getStreamKey(streamStatus);
          if (allStreamIdentities.has(streamKey) === false) {
            allStreamIdentities.set(streamKey, {
              streamName: streamStatus.streamName,
              streamNamespace: streamStatus.streamNamespace,
            });
          }

          return { bucket, allStreamIdentities };
        },
        { bucket: bucketedEntries[entry.jobCreatedAt], allStreamIdentities }
      );

      return acc;
    },
    {
      bucketedEntries: dateBuckets,
      allStreamIdentities: new Map(),
    }
  );

  // ensure each date bucket has an entry for each stream and that they align between days
  const dateBucketKeys: Array<keyof typeof bucketedEntries> = Object.keys(bucketedEntries);
  const streamIdentities = Array.from(allStreamIdentities.values()).sort(sortStreams);

  // entries in the graph's expected format
  const uptimeData: UptimeDayEntry[] = [];

  dateBucketKeys.forEach((dateBucketKey) => {
    const dateEntries = bucketedEntries[dateBucketKey];
    dateEntries.streams.sort(sortStreams);

    uptimeData.push({
      jobId: dateEntries.jobId,
      date: parseInt(dateBucketKey, 10) * 1000,
      runtimeMs: dateEntries.runtimeMs,
      recordsEmitted: dateEntries.recordsEmitted,
      recordsCommitted: dateEntries.recordsCommitted,
      streams: dateEntries.streams,
    });
  });

  return { uptimeData, streamIdentities };
};

// wrapped in memo to avoid redrawing the chart when the component tree re-renders
export const UptimeStatusGraph: React.FC = React.memo(() => {
  // read color values from the theme
  const [colorMap, setColorMap] = useState<Record<string, string>>({});
  const { colorValues } = useAirbyteTheme();
  useEffect(() => {
    const colorMap: Record<string, string> = {
      green: colorValues[styles.greenVar],
      darkBlue: colorValues[styles.darkBlueVar],
      red: colorValues[styles.redVar],
      black: colorValues[styles.blackVar],
      blue: colorValues[styles.blueVar],
      empty: colorValues[styles.emptyVar],
    };
    setColorMap(colorMap);
  }, [colorValues]);

  const { connection } = useConnectionEditService();
  const uptimeHistoryData = useGetConnectionUptimeHistory(connection.connectionId);
  const { isRunning } = useConnectionStatus(connection.connectionId);
  const { data: syncProgressData } = useGetConnectionSyncProgress(connection.connectionId, isRunning);

  const placeholderHistory = useMemo(
    () => generatePlaceholderHistory(isRunning ? syncProgressData : undefined),
    [syncProgressData, isRunning]
  );
  const hasHistoryData = uptimeHistoryData.length > 0 || placeholderHistory.length > 0;

  const { uptimeData, streamIdentities } = useMemo(
    () => formatDataForChart([...uptimeHistoryData, ...placeholderHistory]),
    [placeholderHistory, uptimeHistoryData]
  );

  const maxStreamsCount = Math.max(...uptimeData.map(({ streams: { length } }) => length));

  if (!hasHistoryData) {
    return <NoDataMessage />;
  }

  return (
    <ResponsiveContainer
      width="100%"
      height={Math.max(
        CHART_MIN_HEIGHT,
        Math.min(CHART_MAX_HEIGHT, streamIdentities.length * CHART_STREAM_ROW_HEIGHT + CHART_BASE_HEIGHT)
      )}
    >
      <StreamChart data={uptimeData}>
        <UpdateTooltipTickPositions />

        {colorMap && (
          <Waffle
            maxStreamsCount={maxStreamsCount}
            colorMap={colorMap}
            dataKey={"date" /* without `dataKey` the tooltip won't show */}
            streamsCount={maxStreamsCount}
          />
        )}

        <XAxis dataKey="date" {...xAxisConfig} />

        <Tooltip
          wrapperStyle={{ outline: "none", zIndex: styles.tooltipZindex }}
          content={UptimeStatusGraphTooltip}
          cursor={false /* Waffle handles the cursor rendering */}
          {...tooltipConfig}
        />

        {/* last so it draws on top of all other elements and is clickable everywhere */}
        <ClickToJob {...({} as ComponentPropsWithoutRef<typeof ClickToJob>)} />
      </StreamChart>
    </ResponsiveContainer>
  );
});
UptimeStatusGraph.displayName = "UptimeStatusGraph";
