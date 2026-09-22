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
  committedLabelText: string;
  committedLabelBackground: string;
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
const REFERENCE_LABEL_FONT_SIZE = 10;
const REFERENCE_LABEL_PADDING_X = 6;
const REFERENCE_LABEL_PADDING_Y = 3;
const REFERENCE_LABEL_HEIGHT = REFERENCE_LABEL_FONT_SIZE + REFERENCE_LABEL_PADDING_Y * 2;
const REFERENCE_LABEL_CHAR_WIDTH = 5.5;
const REFERENCE_LABEL_OFFSET = 4;

interface ReferenceLineLabelProps {
  viewBox?: { x?: number; y?: number; width?: number; height?: number };
  label: string;
  textColor: string;
  backgroundColor: string;
  borderColor: string;
}

const ReferenceLineLabel = ({ viewBox, label, textColor, backgroundColor, borderColor }: ReferenceLineLabelProps) => {
  const { x = 0, y = 0, width = 0 } = viewBox ?? {};
  const labelWidth = label.length * REFERENCE_LABEL_CHAR_WIDTH + REFERENCE_LABEL_PADDING_X * 2;
  const rectX = x + width - labelWidth;
  const rectYAbove = y - REFERENCE_LABEL_HEIGHT - REFERENCE_LABEL_OFFSET;
  const rectY = rectYAbove >= 0 ? rectYAbove : y + REFERENCE_LABEL_OFFSET;

  return (
    <g>
      <rect
        x={rectX}
        y={rectY}
        width={labelWidth}
        height={REFERENCE_LABEL_HEIGHT}
        rx={REFERENCE_LABEL_HEIGHT / 2}
        fill={backgroundColor}
        stroke={borderColor}
        strokeWidth={1}
      />
      <text
        x={rectX + labelWidth / 2}
        y={rectY + REFERENCE_LABEL_HEIGHT / 2}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={REFERENCE_LABEL_FONT_SIZE}
        fontWeight={500}
        fill={textColor}
      >
        {label}
      </text>
    </g>
  );
};

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
    committedLabelText: "",
    committedLabelBackground: "",
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
      committedLabelText: colorValues[styles.committedLabelText],
      committedLabelBackground: colorValues[styles.committedLabelBackground],
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
          {referenceLine && (
            <ReferenceLine
              y={referenceLine.value}
              stroke={colorMap.committedLine}
              strokeDasharray="6 4"
              strokeWidth={1.5}
              ifOverflow="extendDomain"
              label={(labelProps: { viewBox?: ReferenceLineLabelProps["viewBox"] }) => (
                <ReferenceLineLabel
                  viewBox={labelProps.viewBox}
                  label={referenceLine.label}
                  textColor={colorMap.committedLabelText}
                  backgroundColor={colorMap.committedLabelBackground}
                  borderColor={colorMap.committedLine}
                />
              )}
            />
          )}
        </BarChart>
      </ResponsiveContainer>
    </Box>
  );
};
