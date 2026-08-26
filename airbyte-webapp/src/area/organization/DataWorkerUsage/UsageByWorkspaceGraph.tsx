import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useOrganizationWorkerUsage } from "core/api";

import { calculateGraphData, UsageGraphGranularity } from "./calculateGraphData";
import { DataWorkerUsageBarChart } from "./DataWorkerUsageBarChart";
import { GraphTooltip } from "./GraphTooltip";
import styles from "./UsageByWorkspaceGraph.module.scss";

export type UsageTimeRange = "1d" | "1w" | "1m";

interface UsageByWorkspaceGraphProps {
  selectedRegionId: string;
  requestDateRange: [string, string];
  displayRange: [string, string];
  selectedTimeRange: UsageTimeRange;
  committedDataWorkers?: number | null;
}

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

export const UsageByWorkspaceGraph = ({
  selectedRegionId,
  requestDateRange,
  displayRange,
  selectedTimeRange,
  committedDataWorkers,
}: UsageByWorkspaceGraphProps) => {
  const { formatDate, formatMessage } = useIntl();
  const granularity: UsageGraphGranularity = selectedTimeRange === "1m" ? "day" : "hour";
  const allUsage = useOrganizationWorkerUsage({
    startDate: requestDateRange[0],
    endDate: requestDateRange[1],
  });
  const selectedRegionUsage = useMemo(
    () => allUsage?.regions.find((region) => region.id === selectedRegionId),
    [selectedRegionId, allUsage]
  );

  const sortedWorkspaces = useMemo(() => {
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
  }, [displayRange, selectedRegionUsage?.workspaces]);

  const top10Workspaces = useMemo(
    () => sortedWorkspaces.slice(0, 10).map(({ workspace }) => workspace),
    [sortedWorkspaces]
  );
  const otherWorkspaces = useMemo(
    () => sortedWorkspaces.slice(10).map(({ workspace }) => workspace),
    [sortedWorkspaces]
  );
  const hasOtherCategory = otherWorkspaces.length > 0;

  const data = useMemo(
    () =>
      calculateGraphData(
        displayRange,
        granularity,
        selectedRegionUsage,
        top10Workspaces.map(({ id }) => id),
        otherWorkspaces.map(({ id }) => id)
      ),
    [displayRange, granularity, selectedRegionUsage, top10Workspaces, otherWorkspaces]
  );

  const xAxisTicks = useMemo(
    () =>
      data
        .filter((_, index) => index % TICK_STEP_BY_RANGE[selectedTimeRange] === 0)
        .map(({ formattedDate }) => formattedDate),
    [data, selectedTimeRange]
  );

  if (!selectedRegionUsage || selectedRegionUsage.workspaces.length === 0 || sortedWorkspaces.length === 0) {
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
    <DataWorkerUsageBarChart
      data={data}
      xAxisDataKey="formattedDate"
      barDataKey="maxWorkspaceUsage"
      xAxisTicks={xAxisTicks}
      xAxisTickFormatter={(value) =>
        formatDate(
          dayjs(value).toDate(),
          selectedTimeRange === "1d" ? { hour: "numeric" } : { month: "short", day: "numeric" }
        )
      }
      xAxisInterval={0}
      yAxisTickFormatter={(value) => formatMessage({ id: "settings.organization.usage.graph.yAxisTick" }, { value })}
      renderTooltipContent={(barColor) => (
        <GraphTooltip
          regionName={selectedRegionUsage.name}
          top10Workspaces={top10Workspaces}
          hasOtherCategory={hasOtherCategory}
          barColor={barColor}
          granularity={granularity}
        />
      )}
      chartKey={`${selectedRegionId}-${selectedTimeRange}`}
      chartMargin={{ top: 0, right: 0, left: 0, bottom: 0 }}
      tooltipPosition={{ y: 20 }}
      barSize={BAR_SIZE_BY_RANGE[selectedTimeRange]}
      referenceLine={
        committedDataWorkers != null && committedDataWorkers > 0
          ? {
              value: committedDataWorkers,
              label: formatMessage({ id: "settings.organization.usage.graph.committedCapacity" }),
            }
          : undefined
      }
    />
  );
};
