import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useOrganizationWorkerUsage } from "core/api";

import { calculateGraphData } from "./calculateGraphData";
import { DataWorkerUsageBarChart } from "./DataWorkerUsageBarChart";
import { GraphTooltip } from "./GraphTooltip";
import styles from "./UsageByWorkspaceGraph.module.scss";

interface UsageByWorkspaceGraphProps {
  selectedRegionId: string;
  dateRange: [string, string];
  committedDataWorkers?: number | null;
}

export const UsageByWorkspaceGraph = ({
  selectedRegionId,
  dateRange,
  committedDataWorkers,
}: UsageByWorkspaceGraphProps) => {
  const { formatDate, formatMessage } = useIntl();
  const allUsage = useOrganizationWorkerUsage({
    startDate: dateRange[0],
    endDate: dateRange[1],
  });
  const selectedRegionUsage = useMemo(
    () => allUsage?.regions.find((region) => region.id === selectedRegionId),
    [selectedRegionId, allUsage]
  );
  // Sort workspaces by total usage descending and filter out zero-usage workspaces
  const sortedWorkspaces = useMemo(() => {
    const workspacesWithTotals =
      selectedRegionUsage?.workspaces.map((ws) => ({
        workspace: ws,
        totalUsage: ws.dataWorkers.reduce((sum, dw) => sum + dw.used, 0),
      })) ?? [];

    return workspacesWithTotals.filter((w) => w.totalUsage > 0).sort((a, b) => b.totalUsage - a.totalUsage);
  }, [selectedRegionUsage?.workspaces]);

  // Split into top 10 and others
  const top10Workspaces = useMemo(() => sortedWorkspaces.slice(0, 10).map((w) => w.workspace), [sortedWorkspaces]);
  const otherWorkspaces = useMemo(() => sortedWorkspaces.slice(10).map((w) => w.workspace), [sortedWorkspaces]);
  const hasOtherCategory = otherWorkspaces.length > 0;

  const data = useMemo(
    () =>
      calculateGraphData(
        dateRange,
        selectedRegionUsage,
        top10Workspaces.map((w) => w.id),
        otherWorkspaces.map((w) => w.id)
      ),
    [dateRange, selectedRegionUsage, top10Workspaces, otherWorkspaces]
  );

  if (!selectedRegionUsage || selectedRegionUsage.workspaces.length === 0) {
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
      xAxisTickFormatter={(value) => formatDate(dayjs(value).toDate(), { month: "short", day: "numeric" })}
      xAxisInterval={1}
      yAxisTickFormatter={(value) => formatMessage({ id: "settings.organization.usage.graph.yAxisTick" }, { value })}
      renderTooltipContent={(barColor) => (
        <GraphTooltip
          regionName={selectedRegionUsage.name}
          top10Workspaces={top10Workspaces}
          hasOtherCategory={hasOtherCategory}
          barColor={barColor}
        />
      )}
      chartKey={selectedRegionId}
      chartMargin={{ top: 0, right: 0, left: 0, bottom: 0 }}
      tooltipPosition={{ y: 20 }}
      barSize={16}
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
