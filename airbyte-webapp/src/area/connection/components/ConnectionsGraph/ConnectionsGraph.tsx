import dayjs from "dayjs";
import { useEffect, useMemo, useState } from "react";
import { useIntl } from "react-intl";
import { Bar, BarChart, BarProps, CartesianGrid, ResponsiveContainer, XAxis, YAxis, Tooltip } from "recharts";

import { FlexContainer } from "components/ui/Flex";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";

import { useGetConnectionsGraphData } from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { DefaultErrorBoundary } from "core/errors";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import styles from "./ConnectionsGraph.module.scss";
import { ConnectionsGraphTooltip } from "./ConnectionsGraphTooltip";
import { getStartOfFirstWindow } from "./getStartOfFirstWindow";
import { LookbackWindow, lookbackConfigs, LookbackConfiguration } from "./lookbackConfiguration";
import { tooltipConfig } from "../HistoricalOverview/ChartConfig";

interface ConnectionsGraphProps {
  lookback: LookbackWindow;
  connections: WebBackendConnectionListItem[];
}

export const ConnectionsGraph: React.FC<ConnectionsGraphProps> = ({ lookback, connections }) => {
  const { formatDate } = useIntl();
  const connectionIds = connections.map((connection) => connection.connectionId);
  const { data } = useGetConnectionsGraphData({
    connectionIds,
    eventTypes: [
      "SYNC_SUCCEEDED",
      "SYNC_INCOMPLETE",
      "SYNC_FAILED",
      "REFRESH_SUCCEEDED",
      "REFRESH_FAILED",
      "REFRESH_INCOMPLETE",
    ],
    createdAtStart: getStartOfFirstWindow(lookback).format(),
    // Intentionally overshooting to the end of the day so we get all current events and so that this query key does
    // not change frequently (which would cause the query to be re-run)
    createdAtEnd: dayjs().endOf("day").format(),
  });

  const [colorMap, setColorMap] = useState<ColorMap>({
    success: "",
    partialSuccess: "",
    error: "",
    running: "",
    axisLabel: "",
    gridLine: "",
    barHover: "",
  });
  const { colorValues } = useAirbyteTheme();

  useEffect(() => {
    const colorMap: ColorMap = {
      success: colorValues[styles.greenVar],
      partialSuccess: colorValues[styles.yellowVar],
      error: colorValues[styles.redVar],
      running: colorValues[styles.blueVar],
      axisLabel: colorValues[styles.axisLabel],
      gridLine: colorValues[styles.gridLine],
      barHover: colorValues[styles.barHover],
    };
    setColorMap(colorMap);
  }, [colorValues]);

  const stackedBarSections: Array<{ dataKey: keyof StackedBarSections; fill: string }> = useMemo(
    () => [
      { dataKey: "success", fill: colorMap.success },
      { dataKey: "partialSuccess", fill: colorMap.partialSuccess },
      { dataKey: "failure", fill: colorMap.error },
      { dataKey: "running", fill: colorMap.running },
    ],
    [colorMap]
  );

  const barChartCategories: BarChartCategory[] = useMemo(() => {
    const windows = createBarCategories(lookback);
    if (data?.events) {
      data.events.forEach((event) => {
        const syncDate = dayjs(event.createdAt);
        const window = windows.find((window) => {
          // "[)" means inclusive start, exclusive end: https://day.js.org/docs/en/plugin/is-between
          return syncDate.isBetween(window.windowStart, window.windowEnd, null, "[)");
        });

        if (window) {
          switch (event.eventType) {
            case "REFRESH_SUCCEEDED":
            case "SYNC_SUCCEEDED":
              window.success++;
              break;
            case "REFRESH_INCOMPLETE":
            case "SYNC_INCOMPLETE":
              window.partialSuccess++;
              break;
            case "REFRESH_FAILED":
            case "SYNC_FAILED":
              window.failure++;
              break;
          }
        }
      });
    }
    return windows;
  }, [lookback, data]);

  const ticks = useMemo(
    () =>
      barChartCategories
        .filter((category, index) => index % category.lookbackConfig.windowsPerTick === 0)
        .map((window) => window.windowId),
    [barChartCategories]
  );

  if (!data) {
    return (
      <FlexContainer direction="column" justifyContent="center" className={styles.connectionsGraph__loadingSkeleton}>
        <LoadingSkeleton />
        <LoadingSkeleton />
      </FlexContainer>
    );
  }

  return (
    <DefaultErrorBoundary>
      <ResponsiveContainer width="99%" height={120} className={styles.connectionsGraph}>
        <BarChart data={barChartCategories} barCategoryGap={1}>
          <XAxis
            ticks={ticks}
            tickFormatter={(value) => formatDate(value, lookbackConfigs[lookback].tickDateFormatOptions)}
            dataKey="windowId"
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 12 }}
          />
          <YAxis
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 12 }}
            minTickGap={10}
            width={30}
            allowDecimals={false}
          />
          <Tooltip
            wrapperStyle={{ outline: "none", zIndex: styles.tooltipZindex }}
            content={ConnectionsGraphTooltip}
            cursor={{ fill: colorMap.barHover }}
            {...tooltipConfig}
          />
          <CartesianGrid stroke={colorMap.gridLine} vertical={false} />
          {stackedBarSections.map((section, index) => (
            <Bar
              key={section.dataKey}
              stackId="a" // All bars are on the same vertical stack - id is arbitrary
              barSize={20}
              isAnimationActive={false}
              dataKey={section.dataKey}
              fill={section.fill}
              shape={(props: BarProps) => {
                const gap = 2;
                const { fill, x, y, width, height } = props;

                const isFirst = index === 0;
                const isLast = index === stackedBarSections.length - 1;

                return (
                  <rect
                    fill={fill}
                    x={x}
                    y={isLast ? y : Number(y) + gap / 2}
                    width={width}
                    height={isLast || isFirst ? Number(height) - gap / 2 : Number(height) - gap}
                  />
                );
              }}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
      <div />
    </DefaultErrorBoundary>
  );
};

type ColorMap = Record<
  "success" | "partialSuccess" | "error" | "running" | "axisLabel" | "barHover" | "gridLine",
  string
>;

type BarChartCategory = {
  // The lookbackConfig is attached to each category for easy access to the lookback window in the tooltip
  lookbackConfig: LookbackConfiguration;
  windowId: number;
  windowStart: Date;
  windowEnd: Date;
} & StackedBarSections;

interface StackedBarSections {
  success: number;
  partialSuccess: number;
  failure: number;
  running: number;
}

const createBarCategories = (lookback: LookbackWindow): BarChartCategory[] => {
  const data: BarChartCategory[] = [];
  const now = dayjs();
  let cursor = getStartOfFirstWindow(lookback);
  const { windowLength, unit } = lookbackConfigs[lookback];

  while (cursor.isBefore(now)) {
    data.push({
      windowId: cursor.valueOf(), // A unique number value to identify the window
      windowStart: cursor.toDate(),
      windowEnd: cursor.add(windowLength, unit).toDate(),
      success: 0,
      partialSuccess: 0,
      failure: 0,
      running: 0,
      lookbackConfig: lookbackConfigs[lookback],
    });
    cursor = cursor.add(windowLength, unit);
  }

  return data;
};
