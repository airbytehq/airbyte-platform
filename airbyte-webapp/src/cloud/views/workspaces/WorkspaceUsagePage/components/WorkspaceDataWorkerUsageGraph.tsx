import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useOrganizationWorkerUsage } from "core/api";

import { WorkspaceDataWorkerGraphTooltip } from "./WorkspaceDataWorkerGraphTooltip";
import styles from "./WorkspaceDataWorkerUsageGraph.module.scss";

const DATE_FORMAT = "YYYY-MM-DD";
const CHART_HEIGHT = 250;

interface HourlyDataPoint {
  date: string;
  used: number;
}

export const WorkspaceDataWorkerUsageGraph: React.FC = () => {
  const workspaceId = useCurrentWorkspaceId();

  const startDate = useMemo(() => dayjs().subtract(7, "day").startOf("day").format(DATE_FORMAT), []);
  const endDate = useMemo(() => dayjs().endOf("day").format(DATE_FORMAT), []);

  const allUsage = useOrganizationWorkerUsage({ startDate, endDate });

  // Extract and aggregate hourly data points for the current workspace across all regions
  const hourlyData: HourlyDataPoint[] = useMemo(() => {
    const hourlyMap = new Map<string, number>();

    allUsage?.regions.forEach((region) => {
      region.workspaces
        .filter((ws) => ws.id === workspaceId)
        .forEach((ws) => {
          ws.dataWorkers.forEach(({ date, used }) => {
            const existing = hourlyMap.get(date) ?? 0;
            hourlyMap.set(date, existing + used);
          });
        });
    });

    return Array.from(hourlyMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, used]) => ({ date, used }));
  }, [allUsage, workspaceId]);

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

  if (hourlyData.length === 0) {
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
    <Box className={styles.graphContainer}>
      <ResponsiveContainer width="99%" height={CHART_HEIGHT}>
        <LineChart data={hourlyData} margin={{ top: 0, right: 20, left: 0, bottom: 0 }}>
          <XAxis
            dataKey="date"
            ticks={dailyTicks}
            tickFormatter={(value) => dayjs(value).format("MMM D")}
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 10 }}
            stroke={styles.tickColor}
            padding={{ left: 20, right: 20 }}
          />
          <YAxis
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 12 }}
            minTickGap={10}
            allowDecimals={false}
            tickMargin={10}
            stroke={styles.tickColor}
          />
          <Tooltip
            content={WorkspaceDataWorkerGraphTooltip}
            wrapperStyle={{ outline: "none", zIndex: styles.tooltipZindex }}
            isAnimationActive={false}
            allowEscapeViewBox={{ x: false, y: true }}
          />
          <CartesianGrid stroke={styles.gridLine} vertical={false} />
          <Line
            type="monotone"
            dataKey="used"
            stroke={styles.lineColor}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4 }}
            animationDuration={300}
          />
        </LineChart>
      </ResponsiveContainer>
    </Box>
  );
};
