import dayjs from "dayjs";
import React, { useEffect, useMemo, useState } from "react";
import { ResponsiveContainer, Tooltip, XAxis } from "recharts";
// these are not worth typing
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore-next-line
import { generateCategoricalChart } from "recharts/es6/chart/generateCategoricalChart";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore-next-line
import { formatAxisMap } from "recharts/es6/util/CartesianUtils";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";

import { getStreamKey } from "area/connection/utils";
import { useGetConnectionUptimeHistory } from "core/api";
import { JobStatus } from "core/api/types/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import styles from "./UptimeStatusGraph.module.scss";
import { UptimeStatusGraphTooltip } from "./UptimeStatusGraphTooltip";
import { ChartStream, UptimeDayEntry, Waffle } from "./WaffleChart";
import {
  CHART_BASE_HEIGHT,
  CHART_MAX_HEIGHT,
  CHART_MIN_HEIGHT,
  CHART_STREAM_ROW_HEIGHT,
} from "../DataMovedGraph/constants";
import { tooltipConfig, xAxisConfig } from "../HistoricalOverview/ChartConfig";
import { NoDataMessage } from "../HistoricalOverview/NoDataMessage";

const StreamChart = generateCategoricalChart({
  chartName: "StreamChart",
  GraphicalChild: [Waffle],
  axisComponents: [{ axisType: "xAxis", AxisComp: XAxis }],
  formatAxisMap,
});

interface SortableStream {
  streamNamespace?: string;
  streamName: string;
}
const sortStreams = (a: SortableStream, b: SortableStream) => {
  const { streamName: nameA, streamNamespace: namespaceA } = a;
  const { streamName: nameB, streamNamespace: namespaceB } = b;

  const namespaceCompare = (namespaceA ?? "")?.localeCompare(namespaceB ?? "");
  if (namespaceCompare !== 0) {
    return namespaceCompare;
  }

  return nameA.localeCompare(nameB);
};

function assertNever(_x: never) {}

const formatDataForChart = (data: ReturnType<typeof useGetConnectionUptimeHistory>) => {
  // bucket entries by their timestamp and collect all stream identities so we can fill in gaps
  const dateBuckets: Record<string, ChartStream[]> = {};
  const today = dayjs();
  for (let i = 0; i < 30; i++) {
    const date = today.subtract(i, "day").startOf("day").unix();
    dateBuckets[date] = [];
  }

  const { bucketedEntries, allStreamIdentities } = data.reduce<{
    bucketedEntries: typeof dateBuckets;
    allStreamIdentities: Map<string, SortableStream>;
  }>(
    (acc, entry) => {
      // not destructuring to avoid creating new objects when returning
      const bucketedEntries = acc.bucketedEntries;
      const allStreamIdentities = acc.allStreamIdentities;

      // add this entry to its bucket
      if (bucketedEntries.hasOwnProperty(entry.timestamp)) {
        let status: ConnectionStatusIndicatorStatus = ConnectionStatusIndicatorStatus.Pending;

        switch (entry.status) {
          case JobStatus.succeeded:
            status = ConnectionStatusIndicatorStatus.OnTime;
            break;
          case JobStatus.failed:
            status = ConnectionStatusIndicatorStatus.Error;
            break;
          case JobStatus.running:
          case JobStatus.cancelled:
          case JobStatus.incomplete:
          case JobStatus.pending:
            status = ConnectionStatusIndicatorStatus.Pending;
            break;
          default:
            assertNever(entry.status);
        }
        bucketedEntries[entry.timestamp].push({
          streamNamespace: entry.streamNamespace,
          streamName: entry.streamName,
          status,
        });
      }

      // add this stream to the map
      const streamKey = getStreamKey(entry);
      if (allStreamIdentities.has(streamKey) === false) {
        allStreamIdentities.set(streamKey, entry);
      }

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
    dateEntries.sort(sortStreams);

    for (let i = 0; i < streamIdentities.length; i++) {
      const streamIdentity = streamIdentities[i];
      const dateEntry = dateEntries[i];

      if (
        !dateEntry ||
        (streamIdentity.streamNamespace ?? "") !== (dateEntry.streamNamespace ?? "") ||
        streamIdentity.streamName !== dateEntry.streamName
      ) {
        dateEntries.splice(i, 0, {
          streamNamespace: streamIdentity.streamNamespace,
          streamName: streamIdentity.streamName,
          status: ConnectionStatusIndicatorStatus.Pending,
        });
      }
    }

    uptimeData.push({
      date: parseInt(dateBucketKey, 10) * 1000,
      streams: dateEntries,
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
      empty: colorValues[styles.emptyVar],
    };
    setColorMap(colorMap);
  }, [colorValues]);

  const { connection } = useConnectionEditService();
  const data = useGetConnectionUptimeHistory(connection.connectionId);
  const hasData = data.length > 0;

  const { uptimeData, streamIdentities } = useMemo(() => formatDataForChart(data), [data]);

  if (!hasData) {
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
        {colorMap && (
          <Waffle
            colorMap={colorMap}
            dataKey={"date" /* without `dataKey` the tooltip won't show */}
            streamsCount={streamIdentities.length}
          />
        )}

        <XAxis dataKey="date" {...xAxisConfig} />

        <Tooltip
          wrapperStyle={{ outline: "none", zIndex: styles.tooltipZindex }}
          content={UptimeStatusGraphTooltip}
          cursor={false /* Waffle handles the cursor rendering */}
          {...tooltipConfig}
        />
      </StreamChart>
    </ResponsiveContainer>
  );
});
UptimeStatusGraph.displayName = "UptimeStatusGraph";
