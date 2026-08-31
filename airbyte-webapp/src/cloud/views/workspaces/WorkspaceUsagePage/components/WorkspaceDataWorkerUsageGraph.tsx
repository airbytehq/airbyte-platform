import classNames from "classnames";
import dayjs from "dayjs";
import { useDeferredValue, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { DataWorkerUsageBarChart } from "area/organization/DataWorkerUsage/DataWorkerUsageBarChart";
import { enumerateTimeBuckets } from "area/organization/DataWorkerUsage/enumerateTimeBuckets";
import { useCurrentWorkspace, useOrganizationHistoricalWorkerUsage, useOrganizationWorkerUsage } from "core/api";
import { useCurrentTime } from "core/utils/time";

import { WorkspaceDataWorkerGraphTooltip } from "./WorkspaceDataWorkerGraphTooltip";
import styles from "./WorkspaceDataWorkerUsageGraph.module.scss";

const DATE_FORMAT = "YYYY-MM-DD";
const HOUR_IN_MS = 60 * 60 * 1000;

type UsageTimeRange = "1d" | "1w" | "1m" | "1q" | "1y";
type UsageGranularity = "hour" | "day" | "week";

interface WorkspaceUsageDataBar {
  date: string;
  used: number;
}

interface WorkspaceComparisonDataBar {
  date: string;
  currentDate?: string;
  previousDate?: string;
  currentUsage: number | null;
  previousUsage: number | null;
}

const TIME_RANGE_OPTIONS: Array<{ labelId: string; value: UsageTimeRange }> = [
  { labelId: "settings.organization.usage.timeRange.1d", value: "1d" },
  { labelId: "settings.organization.usage.timeRange.1w", value: "1w" },
  { labelId: "settings.organization.usage.timeRange.1m", value: "1m" },
  { labelId: "settings.organization.usage.timeRange.1q", value: "1q" },
  { labelId: "settings.organization.usage.timeRange.1y", value: "1y" },
];

const TICK_STEP_BY_RANGE: Record<UsageTimeRange, number> = {
  "1d": 6,
  "1w": 24,
  "1m": 5,
  "1q": 15,
  "1y": 1,
};

const BAR_SIZE_BY_RANGE: Record<UsageTimeRange, number> = {
  "1d": 16,
  "1w": 4,
  "1m": 16,
  "1q": 4,
  "1y": 8,
};

const COMPARISON_BAR_SIZE_BY_RANGE: Record<UsageTimeRange, number> = {
  "1d": 8,
  "1w": 2,
  "1m": 8,
  "1q": 4,
  "1y": 4,
};

const USAGE_UPDATE_INTERVAL_BY_RANGE: Record<UsageTimeRange, number> = {
  "1d": 60_000,
  "1w": 60_000,
  "1m": 60_000,
  "1q": 300_000,
  "1y": 300_000,
};

const startOfCalendarWeek = (date: dayjs.Dayjs) => date.startOf("day").subtract(date.day(), "day");

export const WorkspaceDataWorkerUsageGraph: React.FC = () => {
  const { workspaceId, name: workspaceName } = useCurrentWorkspace();
  const { formatDate, formatMessage } = useIntl();
  const [selectedTimeRange, setSelectedTimeRange] = useState<UsageTimeRange>("1w");
  const [comparisonEnabled, setComparisonEnabled] = useState(false);
  const timeRangeOptions = useMemo(
    () => TIME_RANGE_OPTIONS.map((option) => ({ ...option, label: formatMessage({ id: option.labelId }) })),
    [formatMessage]
  );

  const currentTime = useCurrentTime(USAGE_UPDATE_INTERVAL_BY_RANGE[selectedTimeRange]);

  const timeWindow = useMemo(() => {
    const granularity: UsageGranularity =
      selectedTimeRange === "1y" ? "week" : selectedTimeRange === "1m" || selectedTimeRange === "1q" ? "day" : "hour";
    const boundaryGranularity = granularity === "hour" ? "hour" : "day";
    const rangeEnd = dayjs(currentTime).startOf(boundaryGranularity).add(1, boundaryGranularity);
    const rangeStart =
      selectedTimeRange === "1d"
        ? rangeEnd.subtract(24, "hour")
        : selectedTimeRange === "1w"
        ? rangeEnd.subtract(7 * 24, "hour")
        : selectedTimeRange === "1m"
        ? rangeEnd.subtract(1, "month")
        : selectedTimeRange === "1q"
        ? rangeEnd.subtract(3, "month")
        : rangeEnd.subtract(1, "year");
    const displayRange: [string, string] = [rangeStart.toISOString(), rangeEnd.toISOString()];
    const requestDateRange: [string, string] = [
      rangeStart.toISOString().slice(0, 10),
      rangeEnd.subtract(1, "millisecond").toISOString().slice(0, 10),
    ];
    const historicalRangeEnd = rangeStart;
    const historicalRangeStart =
      selectedTimeRange === "1d"
        ? historicalRangeEnd.subtract(24, "hour")
        : selectedTimeRange === "1w"
        ? historicalRangeEnd.subtract(7 * 24, "hour")
        : selectedTimeRange === "1m"
        ? historicalRangeEnd.subtract(1, "month")
        : selectedTimeRange === "1q"
        ? historicalRangeEnd.subtract(3, "month")
        : historicalRangeEnd.subtract(1, "year");
    const historicalDisplayRange: [string, string] = [
      historicalRangeStart.toISOString(),
      historicalRangeEnd.toISOString(),
    ];
    const historicalRequestDateRange: [string, string] = [
      historicalRangeStart.toISOString().slice(0, 10),
      historicalRangeEnd.subtract(1, "millisecond").toISOString().slice(0, 10),
    ];

    return {
      displayRange,
      granularity,
      requestDateRange,
      historicalDisplayRange,
      historicalRequestDateRange,
      timeRange: selectedTimeRange,
    };
  }, [selectedTimeRange, currentTime]);
  // The clock tick re-anchors the window with a plain setState, outside any transition.
  // Deferring the derived window keeps the mounted chart visible while the re-keyed
  // usage query suspends (at UTC midnight the request dates change and there is no cache).
  const {
    displayRange,
    granularity,
    requestDateRange,
    historicalDisplayRange,
    historicalRequestDateRange,
    timeRange: displayedTimeRange,
  } = useDeferredValue(timeWindow);
  const usageUpdateInterval = USAGE_UPDATE_INTERVAL_BY_RANGE[displayedTimeRange];
  const isLoadingTimeRange = selectedTimeRange !== displayedTimeRange;

  const allUsage = useOrganizationWorkerUsage(
    {
      startDate: requestDateRange[0],
      endDate: requestDateRange[1],
    },
    usageUpdateInterval
  );
  const {
    data: historicalUsage,
    isError: isHistoricalUsageError,
    isInitialLoading: isLoadingHistoricalUsage,
  } = useOrganizationHistoricalWorkerUsage(
    {
      startDate: historicalRequestDateRange[0],
      endDate: historicalRequestDateRange[1],
    },
    { enabled: comparisonEnabled }
  );
  const comparisonReady = comparisonEnabled && historicalUsage !== undefined && !isHistoricalUsageError;

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

    const peakUsageByBucket = new Map<string, number>();
    hourlyUsage.forEach((used, timestamp) => {
      const date = dayjs(timestamp);
      const bucketKey =
        granularity === "week" ? startOfCalendarWeek(date).format(DATE_FORMAT) : date.format(DATE_FORMAT);
      peakUsageByBucket.set(bucketKey, Math.max(peakUsageByBucket.get(bucketKey) ?? 0, used));
    });

    return {
      data: buckets.map((bucket) => ({
        date: bucket.toISOString(),
        used: peakUsageByBucket.get(bucket.format(DATE_FORMAT)) ?? 0,
      })),
      hasCurrentWorkspaceUsage,
    };
  }, [allUsage, displayRange, granularity, workspaceId]);

  const { data: historicalData, hasHistoricalWorkspaceUsage } = useMemo(() => {
    if (!comparisonReady) {
      return { data: [], hasHistoricalWorkspaceUsage: false };
    }

    const rangeStartTimestamp = dayjs(historicalDisplayRange[0]).valueOf();
    const rangeEndTimestamp = dayjs(historicalDisplayRange[1]).valueOf();
    const buckets = enumerateTimeBuckets(
      [historicalDisplayRange[0], dayjs(historicalDisplayRange[1]).subtract(1, "millisecond").toISOString()],
      granularity
    );
    const hourlyUsage = new Map<number, number>();
    let hasHistoricalWorkspaceUsage = false;

    historicalUsage?.regions.forEach((region) => {
      region.workspaces
        .filter((ws) => ws.id === workspaceId)
        .forEach((ws) => {
          ws.dataWorkers.forEach(({ date, used }) => {
            const timestamp = dayjs(date).valueOf();
            if (timestamp < rangeStartTimestamp || timestamp >= rangeEndTimestamp) {
              return;
            }

            const hour = rangeStartTimestamp + Math.floor((timestamp - rangeStartTimestamp) / HOUR_IN_MS) * HOUR_IN_MS;
            hasHistoricalWorkspaceUsage = true;
            hourlyUsage.set(hour, (hourlyUsage.get(hour) ?? 0) + used);
          });
        });
    });

    if (granularity === "hour") {
      return {
        data: buckets.map((bucket) => ({ date: bucket.toISOString(), used: hourlyUsage.get(bucket.valueOf()) ?? 0 })),
        hasHistoricalWorkspaceUsage,
      };
    }

    const peakUsageByBucket = new Map<string, number>();
    hourlyUsage.forEach((used, timestamp) => {
      const date = dayjs(timestamp);
      const bucketKey =
        granularity === "week" ? startOfCalendarWeek(date).format(DATE_FORMAT) : date.format(DATE_FORMAT);
      peakUsageByBucket.set(bucketKey, Math.max(peakUsageByBucket.get(bucketKey) ?? 0, used));
    });

    return {
      data: buckets.map((bucket) => ({
        date: bucket.toISOString(),
        used: peakUsageByBucket.get(bucket.format(DATE_FORMAT)) ?? 0,
      })),
      hasHistoricalWorkspaceUsage,
    };
  }, [comparisonReady, granularity, historicalDisplayRange, historicalUsage, workspaceId]);

  const comparisonData = useMemo<WorkspaceComparisonDataBar[]>(
    () =>
      Array.from({ length: Math.max(data.length, historicalData.length) }, (_, index) => {
        const currentBucket = data[index];
        const historicalBucket = historicalData[index];

        return {
          date: currentBucket?.date ?? historicalBucket.date,
          currentDate: currentBucket?.date,
          previousDate: historicalBucket?.date,
          currentUsage: currentBucket?.used ?? null,
          previousUsage: historicalBucket?.used ?? null,
        };
      }),
    [data, historicalData]
  );

  const chartData: Array<
    Partial<WorkspaceUsageDataBar & WorkspaceComparisonDataBar> & Pick<WorkspaceUsageDataBar, "date">
  > = comparisonReady ? comparisonData : data;

  const xAxisTicks = useMemo(() => {
    if (displayedTimeRange === "1y") {
      return chartData
        .filter(({ date }, index) => index === 0 || dayjs(date).month() !== dayjs(chartData[index - 1].date).month())
        .map(({ date }) => date);
    }

    return chartData.filter((_, index) => index % TICK_STEP_BY_RANGE[displayedTimeRange] === 0).map(({ date }) => date);
  }, [chartData, displayedTimeRange]);

  const hasNoData = comparisonReady
    ? !hasCurrentWorkspaceUsage && !hasHistoricalWorkspaceUsage
    : !hasCurrentWorkspaceUsage;

  return (
    <FlexContainer direction="column" alignItems="stretch" gap="2xl">
      <FlexContainer alignItems="center" gap="md">
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
                onChange={() => setSelectedTimeRange(option.value)}
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
        <FlexItem grow />
        <FlexContainer alignItems="center" gap="sm">
          {/* eslint-disable-next-line jsx-a11y/label-has-associated-control -- the input is rendered by Switch */}
          <label htmlFor="workspace-data-worker-usage-comparison">
            <Text as="span" size="lg">
              <FormattedMessage id="dataWorkerUsage.comparison.toggle" />
            </Text>
          </label>
          <Switch
            id="workspace-data-worker-usage-comparison"
            checked={comparisonEnabled}
            onChange={(event) => setComparisonEnabled(event.target.checked)}
            size="sm"
          />
        </FlexContainer>
      </FlexContainer>
      {comparisonEnabled && isHistoricalUsageError && (
        <FlexContainer alignItems="center" gap="sm">
          <Icon type="infoOutline" color="disabled" />
          <Text color="grey">
            <FormattedMessage id="dataWorkerUsage.comparison.error" />
          </Text>
        </FlexContainer>
      )}
      {isLoadingTimeRange || (comparisonEnabled && isLoadingHistoricalUsage && hasNoData) ? (
        <FlexContainer className={styles.graphContainer} alignItems="center" justifyContent="center">
          <FlexContainer alignItems="center" gap="md">
            <LoadingSpinner />
            <Text>
              <FormattedMessage id="settings.organization.usage.loadingUsageData" />
            </Text>
          </FlexContainer>
        </FlexContainer>
      ) : !hasNoData ? (
        <DataWorkerUsageBarChart
          data={chartData}
          xAxisDataKey="date"
          barDataKey={comparisonReady ? "currentUsage" : "used"}
          comparisonBarDataKey={comparisonReady ? "previousUsage" : undefined}
          xAxisTicks={xAxisTicks}
          xAxisTickFormatter={(value) =>
            formatDate(
              dayjs(value).toDate(),
              displayedTimeRange === "1d"
                ? { hour: "numeric" }
                : displayedTimeRange === "1y"
                ? { month: "short" }
                : { month: "short", day: "numeric" }
            )
          }
          xAxisInterval={0}
          xAxisPadding={{ left: 20, right: 20 }}
          chartKey={`${displayedTimeRange}${comparisonReady ? "-comparison" : ""}`}
          chartMargin={{ top: 0, right: 20, left: 0, bottom: 0 }}
          renderTooltipContent={(barColor, comparisonBarColor) =>
            comparisonReady ? (
              <WorkspaceDataWorkerGraphTooltip
                granularity={granularity}
                comparison={{ barColor, comparisonBarColor, selectedTimeRange: displayedTimeRange }}
              />
            ) : (
              <WorkspaceDataWorkerGraphTooltip workspaceName={workspaceName} granularity={granularity} />
            )
          }
          barSize={
            comparisonReady ? COMPARISON_BAR_SIZE_BY_RANGE[displayedTimeRange] : BAR_SIZE_BY_RANGE[displayedTimeRange]
          }
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
