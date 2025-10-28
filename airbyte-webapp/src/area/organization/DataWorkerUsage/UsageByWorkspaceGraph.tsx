import { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
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
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import { calculateGraphData } from "./calculateGraphData";
import { GraphLegend } from "./GraphLegend";
import { GraphTooltip } from "./GraphTooltip";
import styles from "./UsageByWorkspaceGraph.module.scss";

interface UsageByWorkspaceGraphProps {
  selectedRegionId: string;
  dateRange: [string, string];
}

interface ColorMap {
  gridLine: string;
  workspaceColor1: string;
  workspaceColor2: string;
  workspaceColor3: string;
  barHover: string;
}

const tooltipConfig: TooltipProps<number, string> = {
  animationDuration: 300,
  animationEasing: "ease-out",
  allowEscapeViewBox: { x: false, y: true },
  isAnimationActive: false,
};

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
    workspaceColor1: "",
    workspaceColor2: "",
    workspaceColor3: "",
    barHover: "",
  });
  const { colorValues } = useAirbyteTheme();

  useEffect(() => {
    const colorMap: ColorMap = {
      gridLine: colorValues[styles.gridLine],
      workspaceColor1: colorValues[styles.workspaceColor1],
      workspaceColor2: colorValues[styles.workspaceColor2],
      workspaceColor3: colorValues[styles.workspaceColor3],
      barHover: colorValues[styles.barHover],
    };
    setColorMap(colorMap);
  }, [colorValues]);

  const { formatDate } = useIntl();

  const stackedWorkspaceSections: Array<{ dataKey: string; fill: string; name: string }> = useMemo(
    () =>
      selectedRegionUsage?.workspaces.map((workspaceUsage, index) => {
        const colorOptions = [colorMap.workspaceColor1, colorMap.workspaceColor2, colorMap.workspaceColor3];
        const color = colorOptions[index % colorOptions.length];
        return {
          dataKey: `workspaceUsage.${workspaceUsage.id}`,
          name: workspaceUsage.name,
          fill: color,
        };
      }) ?? [],
    [colorMap.workspaceColor1, colorMap.workspaceColor2, colorMap.workspaceColor3, selectedRegionUsage?.workspaces]
  );

  const data = useMemo(() => calculateGraphData(dateRange, selectedRegionUsage), [dateRange, selectedRegionUsage]);

  const graphHeight = useMemo(() => {
    const numberOfWorkspaces = selectedRegionUsage?.workspaces.length ?? 0;
    return 200 + numberOfWorkspaces * 18.89;
  }, [selectedRegionUsage?.workspaces.length]);

  const animationDuration = useMemo(() => {
    const numberOfWorkspaces = Math.max(1, selectedRegionUsage?.workspaces.length ?? 0);
    return 300 / numberOfWorkspaces;
  }, [selectedRegionUsage?.workspaces.length]);

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
      <ResponsiveContainer width="99%" minHeight={graphHeight}>
        <BarChart
          data={data}
          margin={{
            top: 0,
            right: 0,
            left: 0,
            bottom: 0,
          }}
        >
          <Legend
            verticalAlign="bottom"
            iconType="circle"
            layout="vertical"
            wrapperStyle={{ left: 0 }}
            content={<GraphLegend />}
          />
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
