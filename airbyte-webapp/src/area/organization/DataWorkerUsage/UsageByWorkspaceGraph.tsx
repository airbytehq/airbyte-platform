import dayjs from "dayjs";
import { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  TooltipProps,
  XAxis,
  YAxis,
} from "recharts";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useOrganizationWorkerUsage } from "core/api";
import { useAirbyteTheme } from "core/utils/useAirbyteTheme";

import { calculateGraphData } from "./calculateGraphData";
import { GraphTooltip } from "./GraphTooltip";
import styles from "./UsageByWorkspaceGraph.module.scss";

interface UsageByWorkspaceGraphProps {
  selectedRegionId: string;
  dateRange: [string, string];
  committedDataWorkers?: number | null;
}

interface ColorMap {
  gridLine: string;
  barColor: string;
  barHover: string;
  committedLine: string;
}

const tooltipConfig: TooltipProps<number, string> = {
  animationDuration: 300,
  animationEasing: "ease-out",
  allowEscapeViewBox: { x: false, y: true },
  isAnimationActive: false,
};

const BASE_CHART_HEIGHT = 250;

export const UsageByWorkspaceGraph = ({
  selectedRegionId,
  dateRange,
  committedDataWorkers,
}: UsageByWorkspaceGraphProps) => {
  const { formatMessage } = useIntl();
  const allUsage = useOrganizationWorkerUsage({
    startDate: dateRange[0],
    endDate: dateRange[1],
  });
  const selectedRegionUsage = useMemo(
    () => allUsage?.regions.find((region) => region.id === selectedRegionId),
    [selectedRegionId, allUsage]
  );
  const [colorMap, setColorMap] = useState<ColorMap>({
    gridLine: "",
    barColor: "",
    barHover: "",
    committedLine: "",
  });
  const { colorValues } = useAirbyteTheme();

  useEffect(() => {
    const colorMap: ColorMap = {
      gridLine: colorValues[styles.gridLine],
      barColor: colorValues[styles.barColor],
      barHover: colorValues[styles.barHover],
      committedLine: colorValues[styles.committedLine],
    };
    setColorMap(colorMap);
  }, [colorValues]);

  const { formatDate } = useIntl();

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
    <Box className={styles.usageByWorkspaceGraph}>
      <ResponsiveContainer width="99%" height={BASE_CHART_HEIGHT} key={selectedRegionId}>
        <BarChart
          data={data}
          margin={{
            top: 0,
            right: 0,
            left: 0,
            bottom: 0,
          }}
        >
          <XAxis
            tickFormatter={(value) => formatDate(dayjs(value).toDate(), { month: "short", day: "numeric" })}
            dataKey="formattedDate"
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 10 }}
            interval={1}
          />
          <YAxis
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 12 }}
            tickFormatter={(value) => formatMessage({ id: "settings.organization.usage.graph.yAxisTick" }, { value })}
            minTickGap={10}
            allowDecimals={false}
            tickMargin={10}
          />

          <Tooltip
            wrapperStyle={{ outline: "none", zIndex: styles.tooltipZindex }}
            position={{ y: 20 }}
            content={
              <GraphTooltip
                regionName={selectedRegionUsage.name}
                top10Workspaces={top10Workspaces}
                hasOtherCategory={hasOtherCategory}
                barColor={colorMap.barColor}
              />
            }
            cursor={{ fill: colorMap.barHover }}
            {...tooltipConfig}
          />
          <CartesianGrid stroke={colorMap.gridLine} vertical={false} />
          {committedDataWorkers != null && committedDataWorkers > 0 && (
            <ReferenceLine
              y={committedDataWorkers}
              stroke={colorMap.committedLine}
              strokeDasharray="6 4"
              strokeWidth={1.5}
              ifOverflow="extendDomain"
              label={{
                value: formatMessage({ id: "settings.organization.usage.graph.committedCapacity" }),
                position: "insideTopRight",
                fontSize: 10,
                fill: colorMap.committedLine,
              }}
            />
          )}
          <Bar
            dataKey="maxWorkspaceUsage"
            fill={colorMap.barColor}
            barSize={16}
            animationDuration={300}
            animationEasing="linear"
          />
        </BarChart>
      </ResponsiveContainer>
    </Box>
  );
};
