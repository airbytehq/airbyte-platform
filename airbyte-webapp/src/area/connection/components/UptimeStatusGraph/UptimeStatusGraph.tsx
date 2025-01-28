import React, { ComponentPropsWithoutRef, useEffect, useState } from "react";
import { ResponsiveContainer, Tooltip, XAxis } from "recharts";
// these are not worth typing
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore-next-line
import { generateCategoricalChart } from "recharts/es6/chart/generateCategoricalChart";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore-next-line
import { formatAxisMap } from "recharts/es6/util/CartesianUtils";

import { StreamStatusType } from "components/connection/StreamStatusIndicator";

import { getStreamKey } from "area/connection/utils";
import { ConnectionUptimeHistoryRead, JobStatus } from "core/api/types/AirbyteClient";
import { assertNever } from "core/utils/asserts";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import { UpdateTooltipTickPositions } from "./UpdateTooltipTickPositions";
import styles from "./UptimeStatusGraph.module.scss";
import { UptimeStatusGraphTooltip } from "./UptimeStatusGraphTooltip";
import { ChartStream, UptimeDayEntry, Waffle } from "./WaffleChart";
import { ClickToJob, tooltipConfig, getXAxisConfig } from "../HistoricalOverview/ChartConfig";

const StreamChart = generateCategoricalChart({
  chartName: "StreamChart",
  GraphicalChild: [Waffle],
  axisComponents: [{ axisType: "xAxis", AxisComp: XAxis }],
  formatAxisMap,
});

type SortableStream = Pick<ChartStream, "streamName"> & Partial<Pick<ChartStream, "streamNamespace" | "status">>;

const statusOrder: StreamStatusType[] = [
  StreamStatusType.Pending,
  StreamStatusType.Synced,
  StreamStatusType.Incomplete,
  StreamStatusType.Failed,
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

  // streams that are in the job but don't emit a status are given null values
  return (nameA ?? "").localeCompare(nameB ?? "");
};

interface RunBucket {
  jobId: number;
  runtimeMs: number;
  recordsEmitted: number;
  recordsCommitted: number;
  streams: ChartStream[];
}

export const formatDataForChart = (data: ConnectionUptimeHistoryRead) => {
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
          let status: StreamStatusType = StreamStatusType.Pending;

          switch (streamStatus.status) {
            case JobStatus.succeeded:
              status = StreamStatusType.Synced;
              break;
            case JobStatus.failed:
              status = StreamStatusType.Incomplete;
              break;
            case JobStatus.running:
              status = StreamStatusType.Syncing;
              break;
            case JobStatus.cancelled:
            case JobStatus.incomplete:
            case JobStatus.pending:
              status = StreamStatusType.Pending;
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
  const streamIdentities = Array.from(allStreamIdentities.values())
    .filter((x) => !!x.streamName) // handle edge cases where no steam status was generated for a sync, which introduces an `undefined` stream name here
    .sort(sortStreams);

  // entries in the graph's expected format
  const statusData: UptimeDayEntry[] = [];

  dateBucketKeys.forEach((dateBucketKey, idx) => {
    const dateEntries = ensureStreams(bucketedEntries[dateBucketKey], idx, bucketedEntries, dateBucketKeys);
    dateEntries.streams.sort(sortStreams);

    statusData.push({
      jobId: dateEntries.jobId,
      date: parseInt(dateBucketKey, 10) * 1000,
      runtimeMs: dateEntries.runtimeMs,
      recordsEmitted: dateEntries.recordsEmitted,
      recordsCommitted: dateEntries.recordsCommitted,
      streams: dateEntries.streams,
    });
  });

  return { statusData, streamIdentities };
};

// wrapped in memo to avoid redrawing the chart when the component tree re-renders
export const UptimeStatusGraph: React.FC<{ data: UptimeDayEntry[]; height: number }> = React.memo(
  ({ data, height }) => {
    // read color values from the theme
    const [colorMap, setColorMap] = useState<Record<string, string>>({});
    const { colorValues } = useAirbyteTheme();
    useEffect(() => {
      const colorMap: Record<string, string> = {
        green: colorValues[styles.greenVar],
        darkBlue: colorValues[styles.darkBlueVar],
        red: colorValues[styles.redVar],
        yellow: colorValues[styles.yellowVar],
        blue: colorValues[styles.blueVar],
        empty: colorValues[styles.emptyVar],
      };
      setColorMap(colorMap);
    }, [colorValues]);

    const maxStreamsCount = Math.max(...data.map(({ streams: { length } }) => length));

    return (
      <ResponsiveContainer width="100%" height={height}>
        <StreamChart data={data}>
          <UpdateTooltipTickPositions />

          {colorMap && (
            <Waffle
              maxStreamsCount={maxStreamsCount}
              colorMap={colorMap}
              dataKey={"date" /* without `dataKey` the tooltip won't show */}
              streamsCount={maxStreamsCount}
            />
          )}

          <XAxis dataKey="date" {...getXAxisConfig()} />

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
  }
);
UptimeStatusGraph.displayName = "UptimeStatusGraph";

export function ensureStreams(
  bucket: RunBucket,
  idx: number,
  bucketedEntries: Record<string, RunBucket>,
  dateBucketKeys: string[]
) {
  if (bucket.streams.length === 1 && bucket.streams[0].streamName == null) {
    // there are no stream statuses for this job - this can happen if e.g. the orchestrator falls over before the source emits data
    // find streams to inject by walking backwards and then forwards
    // walk back
    let foundStreams;
    for (let i = idx - 1; i >= 0; i--) {
      const thisBucket = bucketedEntries[dateBucketKeys[i]];
      if (thisBucket.streams.length === 1 && thisBucket.streams[0].streamName == null) {
        continue;
      }
      foundStreams = thisBucket;
      break;
    }
    // walk forward
    if (!foundStreams) {
      for (let i = idx + 1; i < dateBucketKeys.length; i++) {
        const thisBucket = bucketedEntries[dateBucketKeys[i]];
        if (thisBucket.streams.length === 1 && thisBucket.streams[0].streamName == null) {
          continue;
        }
        console.log("found forwards at", i);
        foundStreams = thisBucket;
        break;
      }
    }

    if (foundStreams) {
      return {
        ...bucket,
        streams: foundStreams.streams.map((stream) => ({
          ...stream,
          status: StreamStatusType.Incomplete,
        })),
      };
    }
  }
  return bucket;
}
