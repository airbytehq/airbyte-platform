import dayjs from "dayjs";
import { useEffect, useMemo, useState } from "react";
import { FormattedDate, useIntl } from "react-intl";
import { Bar, BarChart, BarProps, CartesianGrid, ResponsiveContainer, XAxis, YAxis, Tooltip } from "recharts";
import { CategoricalChartFunc } from "recharts/types/chart/generateCategoricalChart";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";

import { CONNECTIONS_GRAPH_EVENT_TYPES, useGetConnectionsGraphData } from "core/api";
import {
  ConnectionEventMinimal,
  WebBackendConnectionListItem,
  ConnectionEventType,
} from "core/api/types/AirbyteClient";
import { DefaultErrorBoundary } from "core/errors";
import { useDrawerActions } from "core/services/ui/DrawerService";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import { ConnectionEventsList } from "./ConnectionEventsList";
import styles from "./ConnectionsGraph.module.scss";
import { ConnectionsGraphTooltip } from "./ConnectionsGraphTooltip";
import { getStartOfFirstWindow } from "./getStartOfFirstWindow";
import { LookbackConfiguration, LookbackWindow, lookbackConfigs } from "./lookbackConfiguration";
import { RunningJobEvent, useCurrentlyRunningSyncs } from "./useCurrentlyRunningSyncs";
import { useTrackConnectionsGraph, useTrackConnectionsGraphLoaded } from "./useTrackConnectionGraph";
import { tooltipConfig } from "../HistoricalOverview/ChartConfig";

interface ConnectionsGraphProps {
  lookback: LookbackWindow;
  connections: WebBackendConnectionListItem[];
}

interface BackendEvent extends ConnectionEventMinimal {
  eventType: (typeof CONNECTIONS_GRAPH_EVENT_TYPES)[number];
}

export type GraphEvent = BackendEvent | RunningJobEvent;

const isGraphEvent = (event: ConnectionEventMinimal | RunningJobEvent): event is GraphEvent => {
  return (
    event.eventType === "RUNNING_JOB" ||
    CONNECTIONS_GRAPH_EVENT_TYPES.includes(event.eventType as (typeof CONNECTIONS_GRAPH_EVENT_TYPES)[number])
  );
};

export const ConnectionsGraph: React.FC<ConnectionsGraphProps> = ({ lookback, connections }) => {
  const { openDrawer } = useDrawerActions();

  const { formatDate } = useIntl();
  const connectionIds = connections.map((connection) => connection.connectionId);
  const currentlyRunningEvents = useCurrentlyRunningSyncs(connectionIds);
  const { data: timelineData } = useGetConnectionsGraphData({
    connectionIds,
    eventTypes: [...CONNECTIONS_GRAPH_EVENT_TYPES],
    createdAtStart: getStartOfFirstWindow(lookback).format(),
    // Intentionally overshooting to the end of the day so we get all current events and so that this query key does
    // not change frequently (which would cause the query to be re-run)
    createdAtEnd: dayjs().endOf("day").format(),
  });

  const { trackConnectionGraphDrawerOpened } = useTrackConnectionsGraph();
  useTrackConnectionsGraphLoaded();

  const combinedEvents = useMemo(() => {
    if (timelineData === undefined || currentlyRunningEvents === undefined) {
      return undefined;
    }

    return [...timelineData.events, ...currentlyRunningEvents];
  }, [timelineData, currentlyRunningEvents]);

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
    if (combinedEvents) {
      combinedEvents.forEach((syncEvent) => {
        const syncDate = dayjs(syncEvent.createdAt);
        const window = windows.find((window) => {
          // "[)" means inclusive start, exclusive end: https://day.js.org/docs/en/plugin/is-between
          return syncDate.isBetween(window.windowStart, window.windowEnd, null, "[)");
        });

        if (window && isGraphEvent(syncEvent)) {
          window.events.push(syncEvent);
          switch (syncEvent.eventType) {
            case ConnectionEventType.REFRESH_SUCCEEDED:
            case ConnectionEventType.SYNC_SUCCEEDED:
              window.success++;
              break;
            case ConnectionEventType.REFRESH_INCOMPLETE:
            case ConnectionEventType.SYNC_INCOMPLETE:
              window.partialSuccess++;
              break;
            case ConnectionEventType.REFRESH_FAILED:
            case ConnectionEventType.SYNC_FAILED:
              window.failure++;
              break;
            case "RUNNING_JOB":
              window.running++;
              break;
          }
        }
      });
    }
    return windows;
  }, [lookback, combinedEvents]);

  const ticks = useMemo(
    () =>
      barChartCategories
        .filter((category, index) => index % category.lookbackConfig.windowsPerTick === 0)
        .map((window) => window.windowId),
    [barChartCategories]
  );

  const handleBarChartClick: CategoricalChartFunc = (chartState) => {
    const { activeLabel } = chartState;
    const activeLabelNumber = Number(activeLabel);
    const categoryClicked = barChartCategories.find((category) => category.windowId === activeLabelNumber);

    if (categoryClicked && categoryClicked.events.length > 0) {
      trackConnectionGraphDrawerOpened({
        success: categoryClicked.success,
        partialSuccess: categoryClicked.partialSuccess,
        failure: categoryClicked.failure,
        running: categoryClicked.running,
      });
      openDrawer({
        title: (
          <Box pl="lg">
            <Heading size="sm" as="h2">
              <FormattedDate value={categoryClicked.windowStart} year="numeric" month="short" day="numeric" />
            </Heading>
          </Box>
        ),
        content: (
          <ConnectionEventsList
            start={categoryClicked.windowStart}
            end={categoryClicked.windowEnd}
            events={categoryClicked.events ?? []}
          />
        ),
      });
    }
  };

  if (combinedEvents === undefined) {
    return (
      <FlexContainer direction="column" justifyContent="center" className={styles.connectionsGraph__loadingSkeleton}>
        <LoadingSkeleton />
        <LoadingSkeleton />
      </FlexContainer>
    );
  }

  return (
    <DefaultErrorBoundary>
      <ResponsiveContainer width="99%" height={120}>
        <BarChart data={barChartCategories} barCategoryGap={1} onClick={handleBarChartClick}>
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
  events: GraphEvent[];
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
      events: [],
    });
    cursor = cursor.add(windowLength, unit);
  }

  return data;
};
