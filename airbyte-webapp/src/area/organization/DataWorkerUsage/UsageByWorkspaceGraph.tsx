import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { useOrganizationHistoricalWorkerUsage, useOrganizationWorkerUsage } from "core/api";

import { calculateGraphData, RegionDataBar, UsageGraphGranularity } from "./calculateGraphData";
import { DataWorkerUsageBarChart } from "./DataWorkerUsageBarChart";
import { GraphTooltip, RegionComparisonDataBar } from "./GraphTooltip";
import styles from "./UsageByWorkspaceGraph.module.scss";

export type UsageTimeRange = "1d" | "1w" | "1m" | "1q" | "1y";

interface UsageByWorkspaceGraphProps {
  selectedRegionId: string;
  requestDateRange: [string, string];
  displayRange: [string, string];
  historicalRequestDateRange: [string, string];
  historicalDisplayRange: [string, string];
  selectedTimeRange: UsageTimeRange;
  comparisonEnabled: boolean;
  committedDataWorkers?: number | null;
}

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

const USAGE_REFETCH_INTERVAL_BY_RANGE: Record<UsageTimeRange, number> = {
  "1d": 60_000,
  "1w": 60_000,
  "1m": 60_000,
  "1q": 300_000,
  "1y": 300_000,
};

export const UsageByWorkspaceGraph = ({
  selectedRegionId,
  requestDateRange,
  displayRange,
  historicalRequestDateRange,
  historicalDisplayRange,
  selectedTimeRange,
  comparisonEnabled,
  committedDataWorkers,
}: UsageByWorkspaceGraphProps) => {
  const { formatDate, formatMessage } = useIntl();
  const granularity: UsageGraphGranularity =
    selectedTimeRange === "1y" ? "week" : selectedTimeRange === "1m" || selectedTimeRange === "1q" ? "day" : "hour";
  const allUsage = useOrganizationWorkerUsage(
    {
      startDate: requestDateRange[0],
      endDate: requestDateRange[1],
    },
    USAGE_REFETCH_INTERVAL_BY_RANGE[selectedTimeRange]
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
  const selectedRegionUsage = useMemo(
    () => allUsage?.regions.find((region) => region.id === selectedRegionId),
    [selectedRegionId, allUsage]
  );
  const historicalSelectedRegionUsage = useMemo(
    () => historicalUsage?.regions.find((region) => region.id === selectedRegionId),
    [selectedRegionId, historicalUsage]
  );

  const hasCurrentRegionSamples = useMemo(() => {
    const rangeStart = dayjs(displayRange[0]).valueOf();
    const rangeEnd = dayjs(displayRange[1]).valueOf();
    return (
      selectedRegionUsage?.workspaces.some((workspace) =>
        workspace.dataWorkers.some(({ date }) => {
          const timestamp = dayjs(date).valueOf();
          return timestamp >= rangeStart && timestamp < rangeEnd;
        })
      ) ?? false
    );
  }, [displayRange, selectedRegionUsage]);

  const hasHistoricalRegionSamples = useMemo(() => {
    const rangeStart = dayjs(historicalDisplayRange[0]).valueOf();
    const rangeEnd = dayjs(historicalDisplayRange[1]).valueOf();
    return (
      historicalSelectedRegionUsage?.workspaces.some((workspace) =>
        workspace.dataWorkers.some(({ date }) => {
          const timestamp = dayjs(date).valueOf();
          return timestamp >= rangeStart && timestamp < rangeEnd;
        })
      ) ?? false
    );
  }, [historicalDisplayRange, historicalSelectedRegionUsage]);

  const sortedWorkspaces = useMemo(() => {
    if (comparisonReady) {
      return [];
    }

    const rangeStart = dayjs(displayRange[0]).valueOf();
    const rangeEnd = dayjs(displayRange[1]).valueOf();
    const workspacesWithTotals =
      selectedRegionUsage?.workspaces.map((workspace) => ({
        workspace,
        totalUsage: workspace.dataWorkers.reduce((sum, dataWorkerUsage) => {
          const timestamp = dayjs(dataWorkerUsage.date).valueOf();
          return timestamp >= rangeStart && timestamp < rangeEnd ? sum + dataWorkerUsage.used : sum;
        }, 0),
      })) ?? [];

    return workspacesWithTotals.filter(({ totalUsage }) => totalUsage > 0).sort((a, b) => b.totalUsage - a.totalUsage);
  }, [comparisonReady, displayRange, selectedRegionUsage?.workspaces]);

  const top10Workspaces = useMemo(
    () => sortedWorkspaces.slice(0, 10).map(({ workspace }) => workspace),
    [sortedWorkspaces]
  );
  const otherWorkspaces = useMemo(
    () => sortedWorkspaces.slice(10).map(({ workspace }) => workspace),
    [sortedWorkspaces]
  );
  const hasOtherCategory = otherWorkspaces.length > 0;

  const currentData = useMemo(
    () =>
      calculateGraphData(
        displayRange,
        granularity,
        selectedRegionUsage,
        comparisonReady ? [] : top10Workspaces.map(({ id }) => id),
        comparisonReady ? [] : otherWorkspaces.map(({ id }) => id)
      ),
    [comparisonReady, displayRange, granularity, selectedRegionUsage, top10Workspaces, otherWorkspaces]
  );

  const historicalData = useMemo(
    () =>
      comparisonReady
        ? calculateGraphData(historicalDisplayRange, granularity, historicalSelectedRegionUsage, [], [])
        : [],
    [comparisonReady, granularity, historicalDisplayRange, historicalSelectedRegionUsage]
  );

  const comparisonData = useMemo<RegionComparisonDataBar[]>(
    () =>
      Array.from({ length: Math.max(currentData.length, historicalData.length) }, (_, index) => {
        const currentBucket = currentData[index];
        const historicalBucket = historicalData[index];

        return {
          formattedDate: currentBucket?.formattedDate ?? historicalBucket.formattedDate,
          currentDate: currentBucket?.formattedDate,
          previousDate: historicalBucket?.formattedDate,
          currentUsage: currentBucket?.regionUsage ?? null,
          previousUsage: historicalBucket?.regionUsage ?? null,
        };
      }),
    [currentData, historicalData]
  );

  const chartData: Array<Partial<RegionDataBar & RegionComparisonDataBar> & Pick<RegionDataBar, "formattedDate">> =
    comparisonReady ? comparisonData : currentData;

  const xAxisTicks = useMemo(() => {
    if (selectedTimeRange === "1y") {
      return chartData
        .filter(
          ({ formattedDate }, index) =>
            index === 0 || dayjs(formattedDate).month() !== dayjs(chartData[index - 1].formattedDate).month()
        )
        .map(({ formattedDate }) => formattedDate);
    }

    return chartData
      .filter((_, index) => index % TICK_STEP_BY_RANGE[selectedTimeRange] === 0)
      .map(({ formattedDate }) => formattedDate);
  }, [chartData, selectedTimeRange]);

  const hasNoData = comparisonReady
    ? !hasCurrentRegionSamples && !hasHistoricalRegionSamples
    : !selectedRegionUsage || selectedRegionUsage.workspaces.length === 0 || sortedWorkspaces.length === 0;

  if (comparisonEnabled && isLoadingHistoricalUsage && hasNoData) {
    return (
      <FlexContainer className={styles.usageByWorkspaceGraph} alignItems="center" justifyContent="center">
        <FlexContainer alignItems="center" gap="md">
          <LoadingSpinner />
          <Text>
            <FormattedMessage id="settings.organization.usage.loadingUsageData" />
          </Text>
        </FlexContainer>
      </FlexContainer>
    );
  }

  if (hasNoData) {
    return (
      <FlexContainer className={styles.usageByWorkspaceGraph} alignItems="center" justifyContent="center">
        <FlexContainer alignItems="center">
          <Icon type="infoOutline" color="disabled" />
          <Text color="grey">
            <FormattedMessage id="settings.organization.usage.noData" />
          </Text>
        </FlexContainer>
      </FlexContainer>
    );
  }

  return (
    <>
      {comparisonEnabled && isHistoricalUsageError && (
        <FlexContainer alignItems="center" gap="sm">
          <Icon type="infoOutline" color="disabled" />
          <Text color="grey">
            <FormattedMessage id="dataWorkerUsage.comparison.error" />
          </Text>
        </FlexContainer>
      )}
      <DataWorkerUsageBarChart
        data={chartData}
        xAxisDataKey="formattedDate"
        barDataKey={comparisonReady ? "currentUsage" : "maxWorkspaceUsage"}
        comparisonBarDataKey={comparisonReady ? "previousUsage" : undefined}
        xAxisTicks={xAxisTicks}
        xAxisTickFormatter={(value) =>
          formatDate(
            dayjs(value).toDate(),
            selectedTimeRange === "1d"
              ? { hour: "numeric" }
              : selectedTimeRange === "1y"
              ? { month: "short" }
              : { month: "short", day: "numeric" }
          )
        }
        xAxisInterval={0}
        yAxisTickFormatter={(value) => formatMessage({ id: "settings.organization.usage.graph.yAxisTick" }, { value })}
        renderTooltipContent={(barColor, comparisonBarColor) =>
          comparisonReady ? (
            <GraphTooltip
              barColor={barColor}
              granularity={granularity}
              comparison={{ comparisonBarColor, selectedTimeRange }}
            />
          ) : (
            <GraphTooltip
              regionName={selectedRegionUsage?.name ?? ""}
              top10Workspaces={top10Workspaces}
              hasOtherCategory={hasOtherCategory}
              barColor={barColor}
              granularity={granularity}
            />
          )
        }
        chartKey={`${selectedRegionId}-${selectedTimeRange}${comparisonReady ? "-comparison" : ""}`}
        chartMargin={{ top: 0, right: 0, left: 0, bottom: 0 }}
        tooltipPosition={{ y: 20 }}
        barSize={
          comparisonReady ? COMPARISON_BAR_SIZE_BY_RANGE[selectedTimeRange] : BAR_SIZE_BY_RANGE[selectedTimeRange]
        }
        referenceLine={
          committedDataWorkers != null && committedDataWorkers > 0
            ? {
                value: committedDataWorkers,
                label: formatMessage({ id: "settings.organization.usage.graph.committedCapacity" }),
              }
            : undefined
        }
      />
    </>
  );
};
