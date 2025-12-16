import { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, TooltipProps, XAxis, YAxis } from "recharts";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useOrganizationWorkerUsage } from "core/api";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import { calculateGraphData } from "./calculateGraphData";
import { GraphTooltip } from "./GraphTooltip";
import styles from "./UsageByWorkspaceGraph.module.scss";
import { getWorkspaceColorByIndex } from "./utils";

interface UsageByWorkspaceGraphProps {
  selectedRegionId: string;
  dateRange: [string, string];
}

interface ColorMap {
  gridLine: string;
  otherColor: string;
  barHover: string;
}

const tooltipConfig: TooltipProps<number, string> = {
  animationDuration: 300,
  animationEasing: "ease-out",
  allowEscapeViewBox: { x: false, y: true },
  isAnimationActive: false,
};

const BASE_CHART_HEIGHT = 250;

export const UsageByWorkspaceGraph = ({ selectedRegionId, dateRange }: UsageByWorkspaceGraphProps) => {
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
    otherColor: "",
    barHover: "",
  });
  const { colorValues } = useAirbyteTheme();

  useEffect(() => {
    const colorMap: ColorMap = {
      gridLine: colorValues[styles.gridLine],
      otherColor: colorValues[styles.otherColor],
      barHover: colorValues[styles.barHover],
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

  // Generate workspace color map with sequential colors from theme
  const workspaceColorMap = useMemo(() => {
    const map = new Map<string, string>();
    top10Workspaces.forEach((workspace, index) => {
      const color = getWorkspaceColorByIndex(index);
      map.set(workspace.id, color);
    });
    return map;
  }, [top10Workspaces]);

  const stackedWorkspaceSections: Array<{ dataKey: string; fill: string; name: string }> = useMemo(() => {
    const sections = top10Workspaces.map((workspace) => ({
      dataKey: `workspaceUsage.${workspace.id}`,
      name: workspace.name,
      fill: workspaceColorMap.get(workspace.id) || "#000",
    }));

    if (hasOtherCategory) {
      sections.push({
        dataKey: "workspaceUsage.other",
        name: "Other",
        fill: colorMap.otherColor,
      });
    }

    return sections;
  }, [top10Workspaces, hasOtherCategory, workspaceColorMap, colorMap.otherColor]);

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

  const animationDuration = useMemo(() => {
    const numberOfWorkspaces = Math.max(1, top10Workspaces.length + (hasOtherCategory ? 1 : 0));
    return 300 / numberOfWorkspaces;
  }, [top10Workspaces.length, hasOtherCategory]);

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
            tickFormatter={(value) => formatDate(value, { month: "short", day: "numeric" })}
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
            content={GraphTooltip}
            cursor={{ fill: colorMap.barHover }}
            {...tooltipConfig}
          />
          <CartesianGrid stroke={colorMap.gridLine} vertical={false} />
          {stackedWorkspaceSections.map((day, index) => (
            <Bar
              stackId="a"
              key={day.dataKey}
              name={day.name}
              dataKey={day.dataKey}
              fill={day.fill}
              barSize={16}
              animationDuration={animationDuration}
              animationBegin={animationDuration * index}
              animationEasing="linear"
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </Box>
  );
};
