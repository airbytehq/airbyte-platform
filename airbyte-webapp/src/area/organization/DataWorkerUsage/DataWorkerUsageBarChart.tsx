import type { ReactElement } from "react";
import type { TooltipProps, XAxisProps, YAxisProps } from "recharts";

import { useEffect, useState } from "react";
import { Bar, BarChart, CartesianGrid, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { Box } from "components/ui/Box";

import { useAirbyteTheme } from "core/utils/useAirbyteTheme";

import styles from "./DataWorkerUsageBarChart.module.scss";

interface ColorMap {
  gridLine: string;
  barColor: string;
  comparisonBarColor: string;
  barHover: string;
  committedLine: string;
  tickColor: string;
}

interface ChartMargin {
  top?: number;
  right?: number;
  bottom?: number;
  left?: number;
}

interface ReferenceLineConfiguration {
  value: number;
  label: string;
}

export interface DataWorkerUsageBarChartProps<ChartData extends object> {
  data: ChartData[];
  xAxisDataKey: Extract<keyof ChartData, string>;
  barDataKey: Extract<keyof ChartData, string>;
  comparisonBarDataKey?: Extract<keyof ChartData, string>;
  renderTooltipContent: (barColor: string, comparisonBarColor: string) => ReactElement;
  xAxisTicks?: XAxisProps["ticks"];
  xAxisTickFormatter?: XAxisProps["tickFormatter"];
  xAxisInterval?: XAxisProps["interval"];
  xAxisPadding?: XAxisProps["padding"];
  yAxisTickFormatter?: YAxisProps["tickFormatter"];
  chartKey?: React.Key;
  chartMargin?: ChartMargin;
  tooltipPosition?: TooltipProps<number, string>["position"];
  barSize?: number;
  referenceLine?: ReferenceLineConfiguration;
}

const tooltipConfig: TooltipProps<number, string> = {
  animationDuration: 300,
  animationEasing: "ease-out",
  allowEscapeViewBox: { x: false, y: true },
  isAnimationActive: false,
};

const BASE_CHART_HEIGHT = 250;

export const DataWorkerUsageBarChart = <ChartData extends object>({
  data,
  xAxisDataKey,
  barDataKey,
  comparisonBarDataKey,
  renderTooltipContent,
  xAxisTicks,
  xAxisTickFormatter,
  xAxisInterval,
  xAxisPadding,
  yAxisTickFormatter,
  chartKey,
  chartMargin,
  tooltipPosition,
  barSize,
  referenceLine,
}: DataWorkerUsageBarChartProps<ChartData>) => {
  const [colorMap, setColorMap] = useState<ColorMap>({
    gridLine: "",
    barColor: "",
    comparisonBarColor: "",
    barHover: "",
    committedLine: "",
    tickColor: "",
  });
  const { colorValues } = useAirbyteTheme();

  useEffect(() => {
    setColorMap({
      gridLine: colorValues[styles.gridLine],
      barColor: colorValues[styles.barColor],
      comparisonBarColor: colorValues[styles.comparisonBarColor],
      barHover: colorValues[styles.barHover],
      committedLine: colorValues[styles.committedLine],
      tickColor: colorValues[styles.tickColor],
    });
  }, [colorValues]);

  return (
    <Box className={styles.dataWorkerUsageBarChart}>
      <ResponsiveContainer width="99%" height={BASE_CHART_HEIGHT} key={chartKey}>
        <BarChart data={data} margin={chartMargin}>
          <XAxis
            dataKey={xAxisDataKey}
            ticks={xAxisTicks}
            tickFormatter={xAxisTickFormatter}
            interval={xAxisInterval}
            padding={xAxisPadding}
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 10 }}
            stroke={colorMap.tickColor}
          />
          <YAxis
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 12 }}
            tickFormatter={yAxisTickFormatter}
            minTickGap={10}
            allowDecimals={false}
            tickMargin={10}
            stroke={colorMap.tickColor}
          />
          <Tooltip
            wrapperStyle={{ outline: "none", zIndex: styles.tooltipZindex }}
            position={tooltipPosition}
            content={renderTooltipContent(colorMap.barColor, colorMap.comparisonBarColor)}
            cursor={{ fill: colorMap.barHover }}
            {...tooltipConfig}
          />
          <CartesianGrid stroke={colorMap.gridLine} vertical={false} />
          {referenceLine && (
            <ReferenceLine
              y={referenceLine.value}
              stroke={colorMap.committedLine}
              strokeDasharray="6 4"
              strokeWidth={1.5}
              ifOverflow="extendDomain"
              label={{
                value: referenceLine.label,
                position: "insideTopRight",
                fontSize: 10,
                fill: colorMap.committedLine,
              }}
            />
          )}
          <Bar
            dataKey={barDataKey}
            fill={colorMap.barColor}
            barSize={barSize}
            animationDuration={300}
            animationEasing="linear"
          />
          {comparisonBarDataKey && (
            <Bar
              dataKey={comparisonBarDataKey}
              fill={colorMap.comparisonBarColor}
              barSize={barSize}
              animationDuration={300}
              animationEasing="linear"
            />
          )}
        </BarChart>
      </ResponsiveContainer>
    </Box>
  );
};
