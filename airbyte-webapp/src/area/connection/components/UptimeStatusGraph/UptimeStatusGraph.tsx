import React, { MutableRefObject, useEffect, useRef, useState } from "react";
import { ResponsiveContainer, Tooltip, XAxis } from "recharts";
// these are not worth typing
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore-next-line
import { generateCategoricalChart } from "recharts/es6/chart/generateCategoricalChart";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore-next-line
import { formatAxisMap } from "recharts/es6/util/CartesianUtils";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";

import styles from "./UptimeStatusGraph.module.scss";
import { UptimeStatusGraphTooltip } from "./UptimeStatusGraphTooltip";
import { UptimeDayEntry, Waffle } from "./WaffleChart";

// Build placeholder data
const STREAMS_COUNT = 20;
const uptimeData: UptimeDayEntry[] = [];
for (let i = 0; i < 30; i++) {
  const date = Date.UTC(2023, 7, i);
  const streams: (typeof uptimeData)[number]["streams"] = [];

  for (let j = 0; j < STREAMS_COUNT; j++) {
    let status: ConnectionStatusIndicatorStatus;

    if (i > 25 && j >= STREAMS_COUNT - 1) {
      // disabled last stream on the last four days
      status = ConnectionStatusIndicatorStatus.Disabled;
    } else if (j === 2 || j === STREAMS_COUNT - 3 || j === Math.floor(STREAMS_COUNT / 2)) {
      // second, middle, and third to last are error
      status = ConnectionStatusIndicatorStatus.Error;
    } else if (j === 4 && i >= 15) {
      // 5th is action required
      status = ConnectionStatusIndicatorStatus.ActionRequired;
    } else if (j === 5 && i >= 10 && i <= 16) {
      // 5th was late for a duration
      status = ConnectionStatusIndicatorStatus.Late;
    } else {
      status = ConnectionStatusIndicatorStatus.OnTime;
    }
    streams.push({ status });
  }

  uptimeData.push({ date, streams });
}

const CHART_MIN_HEIGHT = 50;
const CHART_MAX_HEIGHT = 175;
const CHART_STREAM_ROW_HEIGHT = 15;
const CHART_BASE_HEIGHT = 17;

const StreamChart = generateCategoricalChart({
  chartName: "StreamChart",
  GraphicalChild: [Waffle],
  axisComponents: [{ axisType: "xAxis", AxisComp: XAxis }],
  formatAxisMap,
});

// wrapped in memo to avoid redrawing the chart when the component tree re-renders
export const UptimeStatusGraph: React.FC = React.memo(() => {
  const chartRef = useRef<typeof ResponsiveContainer>();
  const [colorMap, setColorMap] = useState<Record<string, string>>({});

  useEffect(() => {
    // nested currents as ResponsiveContainer's ref is itself a ref 🤦🏻‍♂️
    // https://github.com/recharts/recharts/blob/1346e7123922b6427186ae13d4b0d36b076bc371/src/component/ResponsiveContainer.tsx#L73-L74
    const wrapperDiv = (chartRef as unknown as MutableRefObject<MutableRefObject<HTMLDivElement>>).current?.current;
    if (wrapperDiv) {
      const style = window.getComputedStyle(wrapperDiv);
      const extractVariableNameFromCssVar = /^var\((.*)\)$/;
      const colorMap: Record<string, string> = {
        green: style.getPropertyValue(styles.greenVar.replace(extractVariableNameFromCssVar, "$1")),
        darkBlue: style.getPropertyValue(styles.darkBlueVar.replace(extractVariableNameFromCssVar, "$1")),
        red: style.getPropertyValue(styles.redVar.replace(extractVariableNameFromCssVar, "$1")),
        black: style.getPropertyValue(styles.blackVar.replace(extractVariableNameFromCssVar, "$1")),
        empty: style.getPropertyValue(styles.emptyVar.replace(extractVariableNameFromCssVar, "$1")),
      };
      setColorMap(colorMap);
    }
  }, []);

  return (
    <ResponsiveContainer
      ref={chartRef}
      width="100%"
      height={Math.max(
        CHART_MIN_HEIGHT,
        Math.min(CHART_MAX_HEIGHT, STREAMS_COUNT * CHART_STREAM_ROW_HEIGHT + CHART_BASE_HEIGHT)
      )}
    >
      <StreamChart data={uptimeData}>
        {colorMap && (
          <Waffle
            colorMap={colorMap}
            dataKey={"date" /* without `dataKey` the tooltip won't show */}
            streamsCount={uptimeData[0].streams.length}
          />
        )}

        <XAxis
          domain={["dataMin - 43200000", "dataMax + 43200000"]} // add 12 hours to each side of the chart to avoid trimming the first and last column widths
          tickCount={6}
          interval="preserveStartEnd"
          style={{ fontSize: "10px" }}
          type="number"
          dataKey="date"
          stroke={styles.labelColor}
          tickFormatter={(x) => new Date(x).toLocaleDateString(undefined, { month: "short", day: "numeric" })}
        />

        <Tooltip
          animationDuration={300}
          animationEasing="ease-out"
          allowEscapeViewBox={{ x: false, y: true }}
          wrapperStyle={{ outline: "none", zIndex: styles.tooltipZindex }}
          offset={0}
          content={UptimeStatusGraphTooltip}
          cursor={false}
        />
      </StreamChart>
    </ResponsiveContainer>
  );
});
UptimeStatusGraph.displayName = "UptimeStatusGraph";
