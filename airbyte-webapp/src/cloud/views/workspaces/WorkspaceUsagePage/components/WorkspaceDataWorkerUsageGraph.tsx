import classNames from "classnames";
import dayjs from "dayjs";
import { startTransition, useDeferredValue, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DataWorkerUsageBarChart } from "area/organization/DataWorkerUsage/DataWorkerUsageBarChart";
import { enumerateTimeBuckets } from "area/organization/DataWorkerUsage/enumerateTimeBuckets";
import { useCurrentWorkspace, useOrganizationWorkerUsage } from "core/api";
import { useCurrentTime } from "core/utils/time";

import { WorkspaceDataWorkerGraphTooltip } from "./WorkspaceDataWorkerGraphTooltip";
import styles from "./WorkspaceDataWorkerUsageGraph.module.scss";

const DATE_FORMAT = "YYYY-MM-DD";
const HOUR_IN_MS = 60 * 60 * 1000;

type UsageTimeRange = "1d" | "1w" | "1m";
type UsageGranularity = "hour" | "day";

const TIME_RANGE_OPTIONS: Array<{ labelId: string; value: UsageTimeRange }> = [
  { labelId: "settings.organization.usage.timeRange.1d", value: "1d" },
  { labelId: "settings.organization.usage.timeRange.1w", value: "1w" },
  { labelId: "settings.organization.usage.timeRange.1m", value: "1m" },
];

const TICK_STEP_BY_RANGE: Record<UsageTimeRange, number> = {
  "1d": 6,
  "1w": 24,
  "1m": 5,
};

const BAR_SIZE_BY_RANGE: Record<UsageTimeRange, number> = {
  "1d": 16,
  "1w": 4,
  "1m": 16,
};

export const WorkspaceDataWorkerUsageGraph: React.FC = () => {
  const { workspaceId, name: workspaceName } = useCurrentWorkspace();
  const { formatDate, formatMessage } = useIntl();
  const [selectedTimeRange, setSelectedTimeRange] = useState<UsageTimeRange>("1w");
  const timeRangeOptions = useMemo(
    () => TIME_RANGE_OPTIONS.map((option) => ({ ...option, label: formatMessage({ id: option.labelId }) })),
    [formatMessage]
  );

  const currentTime = useCurrentTime(60_000);

  const timeWindow = useMemo(() => {
    const granularity: UsageGranularity = selectedTimeRange === "1m" ? "day" : "hour";
    const bucketCount = selectedTimeRange === "1d" ? 24 : selectedTimeRange === "1w" ? 7 * 24 : 30;
    const rangeEnd = dayjs(currentTime).startOf(granularity).add(1, granularity);
    const rangeStart = rangeEnd.subtract(bucketCount, granularity);
    const displayRange: [string, string] = [rangeStart.toISOString(), rangeEnd.toISOString()];
    const requestDateRange: [string, string] = [
      rangeStart.toISOString().slice(0, 10),
      rangeEnd.subtract(1, "millisecond").toISOString().slice(0, 10),
    ];

    return { displayRange, granularity, requestDateRange };
  }, [selectedTimeRange, currentTime]);
  // The clock tick re-anchors the window with a plain setState, outside any transition.
  // Deferring the derived window keeps the mounted chart visible while the re-keyed
  // usage query suspends (at UTC midnight the request dates change and there is no cache).
  const { displayRange, granularity, requestDateRange } = useDeferredValue(timeWindow);

  const allUsage = useOrganizationWorkerUsage({
    startDate: requestDateRange[0],
    endDate: requestDateRange[1],
  });

  const { data, hasCurrentWorkspaceUsage } = useMemo(() => {
    const rangeStartTimestamp = dayjs(displayRange[0]).valueOf();
    const rangeEndTimestamp = dayjs(displayRange[1]).valueOf();
    const buckets = enumerateTimeBuckets(
      [displayRange[0], dayjs(displayRange[1]).subtract(1, "millisecond").toISOString()],
      granularity
    );
    const hourlyUsage = new Map<number, number>();
    let hasCurrentWorkspaceUsage = false;

    allUsage?.regions.forEach((region) => {
      region.workspaces
        .filter((ws) => ws.id === workspaceId)
        .forEach((ws) => {
          ws.dataWorkers.forEach(({ date, used }) => {
            const timestamp = dayjs(date).valueOf();
            if (timestamp < rangeStartTimestamp || timestamp >= rangeEndTimestamp) {
              return;
            }

            const hour = rangeStartTimestamp + Math.floor((timestamp - rangeStartTimestamp) / HOUR_IN_MS) * HOUR_IN_MS;
            hasCurrentWorkspaceUsage = true;
            hourlyUsage.set(hour, (hourlyUsage.get(hour) ?? 0) + used);
          });
        });
    });

    if (granularity === "hour") {
      return {
        data: buckets.map((bucket) => ({ date: bucket.toISOString(), used: hourlyUsage.get(bucket.valueOf()) ?? 0 })),
        hasCurrentWorkspaceUsage,
      };
    }

    const dailyPeakUsage = new Map<string, number>();
    hourlyUsage.forEach((used, timestamp) => {
      const date = dayjs(timestamp).format(DATE_FORMAT);
      dailyPeakUsage.set(date, Math.max(dailyPeakUsage.get(date) ?? 0, used));
    });

    return {
      data: buckets.map((bucket) => ({
        date: bucket.toISOString(),
        used: dailyPeakUsage.get(bucket.format(DATE_FORMAT)) ?? 0,
      })),
      hasCurrentWorkspaceUsage,
    };
  }, [allUsage, displayRange, granularity, workspaceId]);

  const xAxisTicks = useMemo(
    () => data.filter((_, index) => index % TICK_STEP_BY_RANGE[selectedTimeRange] === 0).map(({ date }) => date),
    [data, selectedTimeRange]
  );

  return (
    <FlexContainer direction="column" alignItems="stretch" gap="2xl">
      <FlexContainer>
        <fieldset className={styles.workspaceDataWorkerUsageGraph__timeRangeControl}>
          <legend className={styles.workspaceDataWorkerUsageGraph__timeRangeLegend}>
            <FormattedMessage id="settings.organization.usage.timeRange.legend" />
          </legend>
          {timeRangeOptions.map((option) => (
            <label
              key={option.value}
              htmlFor={`workspace-data-worker-usage-time-range-${option.value}`}
              className={classNames(styles.workspaceDataWorkerUsageGraph__timeRangeOption, {
                [styles["workspaceDataWorkerUsageGraph__timeRangeOption--selected"]]:
                  option.value === selectedTimeRange,
              })}
            >
              <input
                id={`workspace-data-worker-usage-time-range-${option.value}`}
                type="radio"
                name="workspace-data-worker-usage-time-range"
                value={option.value}
                checked={selectedTimeRange === option.value}
                onChange={() => startTransition(() => setSelectedTimeRange(option.value))}
                className={styles.workspaceDataWorkerUsageGraph__timeRangeInput}
              />
              <Text
                color={option.value === selectedTimeRange ? "darkBlue" : "grey"}
                className={styles.workspaceDataWorkerUsageGraph__timeRangeLabel}
                as="span"
                size="lg"
                bold
              >
                {option.label}
              </Text>
            </label>
          ))}
        </fieldset>
      </FlexContainer>
      {hasCurrentWorkspaceUsage ? (
        <DataWorkerUsageBarChart
          data={data}
          xAxisDataKey="date"
          barDataKey="used"
          xAxisTicks={xAxisTicks}
          xAxisTickFormatter={(value) =>
            formatDate(
              dayjs(value).toDate(),
              selectedTimeRange === "1d" ? { hour: "numeric" } : { month: "short", day: "numeric" }
            )
          }
          xAxisInterval={0}
          xAxisPadding={{ left: 20, right: 20 }}
          chartKey={selectedTimeRange}
          chartMargin={{ top: 0, right: 20, left: 0, bottom: 0 }}
          renderTooltipContent={() => (
            <WorkspaceDataWorkerGraphTooltip workspaceName={workspaceName} granularity={granularity} />
          )}
          barSize={BAR_SIZE_BY_RANGE[selectedTimeRange]}
          referenceLine={
            allUsage?.committedDataWorkers != null && allUsage.committedDataWorkers > 0
              ? {
                  value: allUsage.committedDataWorkers,
                  label: formatMessage({ id: "settings.organization.usage.graph.committedCapacity" }),
                }
              : undefined
          }
        />
      ) : (
        <FlexContainer className={styles.graphContainer} alignItems="center" justifyContent="center">
          <FlexContainer alignItems="center" gap="sm">
            <Icon type="infoOutline" color="disabled" />
            <Text color="grey">
              <FormattedMessage id="settings.workspace.usage.dataWorker.noData" />
            </Text>
          </FlexContainer>
        </FlexContainer>
      )}
    </FlexContainer>
  );
};
