import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DataWorkerUsageBarChart } from "area/organization/DataWorkerUsage/DataWorkerUsageBarChart";
import { enumerateTimeBuckets } from "area/organization/DataWorkerUsage/enumerateTimeBuckets";
import { useCurrentWorkspace, useOrganizationWorkerUsage } from "core/api";

import { WorkspaceDataWorkerGraphTooltip } from "./WorkspaceDataWorkerGraphTooltip";
import styles from "./WorkspaceDataWorkerUsageGraph.module.scss";

const DATE_FORMAT = "YYYY-MM-DD";

export const WorkspaceDataWorkerUsageGraph: React.FC = () => {
  const { workspaceId, name: workspaceName } = useCurrentWorkspace();
  const { formatMessage } = useIntl();

  const { startDate, endDate, chartStart, chartEnd } = useMemo(() => {
    const now = dayjs();
    const chartStart = now.subtract(7, "day").startOf("day");

    return {
      startDate: chartStart.utc().format(DATE_FORMAT),
      endDate: now.utc().format(DATE_FORMAT),
      chartStart: chartStart.toISOString(),
      chartEnd: now.toISOString(),
    };
  }, []);

  const allUsage = useOrganizationWorkerUsage({ startDate, endDate });

  // Extract and aggregate hourly data points for the current workspace across all regions
  const { hourlyData, hasCurrentWorkspaceUsage } = useMemo(() => {
    const hourlyBuckets = enumerateTimeBuckets([chartStart, chartEnd], "hour");
    const hourlyMap = new Map<string, number>(hourlyBuckets.map((hour) => [hour.toISOString(), 0]));
    const firstBucketTimestamp = hourlyBuckets[0]?.valueOf();
    let hasCurrentWorkspaceUsage = false;

    allUsage?.regions.forEach((region) => {
      region.workspaces
        .filter((ws) => ws.id === workspaceId)
        .forEach((ws) => {
          ws.dataWorkers.forEach(({ date, used }) => {
            if (firstBucketTimestamp === undefined) {
              return;
            }

            const hourIndex = Math.floor((dayjs(date).valueOf() - firstBucketTimestamp) / (60 * 60 * 1000));
            const hourBucket = hourlyBuckets[hourIndex];
            if (!hourBucket) {
              return;
            }

            const hour = hourBucket.toISOString();
            hasCurrentWorkspaceUsage = true;
            const existing = hourlyMap.get(hour) ?? 0;
            hourlyMap.set(hour, existing + used);
          });
        });
    });

    return {
      hourlyData: Array.from(hourlyMap.entries())
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([date, used]) => ({ date, used })),
      hasCurrentWorkspaceUsage,
    };
  }, [allUsage, chartEnd, chartStart, workspaceId]);

  // Compute one tick per unique calendar day, placed at the midpoint (noon) of each day's data range
  const dailyTicks = useMemo(() => {
    const dayGroups = new Map<string, string[]>();
    for (const point of hourlyData) {
      const day = dayjs(point.date).format("YYYY-MM-DD");
      const group = dayGroups.get(day) ?? [];
      group.push(point.date);
      dayGroups.set(day, group);
    }
    const ticks: string[] = [];
    for (const [, entries] of dayGroups) {
      const midIndex = Math.floor(entries.length / 2);
      ticks.push(entries[midIndex]);
    }
    return ticks;
  }, [hourlyData]);

  if (!hasCurrentWorkspaceUsage) {
    return (
      <FlexContainer className={styles.graphContainer} alignItems="center" justifyContent="center">
        <FlexContainer alignItems="center" gap="sm">
          <Icon type="infoOutline" color="disabled" />
          <Text color="grey">
            <FormattedMessage id="settings.workspace.usage.dataWorker.noData" />
          </Text>
        </FlexContainer>
      </FlexContainer>
    );
  }

  return (
    <DataWorkerUsageBarChart
      data={hourlyData}
      xAxisDataKey="date"
      barDataKey="used"
      xAxisTicks={dailyTicks}
      xAxisTickFormatter={(value) => dayjs(value).format("MMM D")}
      xAxisPadding={{ left: 20, right: 20 }}
      chartMargin={{ top: 0, right: 20, left: 0, bottom: 0 }}
      renderTooltipContent={() => <WorkspaceDataWorkerGraphTooltip workspaceName={workspaceName} />}
      referenceLine={
        allUsage?.committedDataWorkers != null && allUsage.committedDataWorkers > 0
          ? {
              value: allUsage.committedDataWorkers,
              label: formatMessage({ id: "settings.organization.usage.graph.committedCapacity" }),
            }
          : undefined
      }
    />
  );
};
