import { TooltipProps, XAxisProps } from "recharts";
import { CategoricalChartState } from "recharts/types/chart/generateCategoricalChart";
import { BaseAxisProps } from "recharts/types/util/types";
import { scaleLinear } from "victory-vendor/d3-scale";

import { useCurrentConnection } from "core/api";
import { useModalService } from "hooks/services/Modal";
import { openJobLogsModal } from "pages/connections/ConnectionTimelinePage/JobEventMenu";

import styles from "./ChartConfig.module.scss";

export const MAX_BAR_WIDTH = 30;

export const getXAxisConfig = (): XAxisProps => {
  // this is the scale used for bar charts with axis[type=number]
  // https://github.com/recharts/recharts/blob/master/src/util/ChartUtils.ts#L760
  // it needs to be created per chart, not shared between them (else https://github.com/airbytehq/airbyte-internal-issues/issues/10407)
  const axisScale = scaleLinear();

  // recharts wants to use the value in `barSize` as the bar width
  // but will scale it down when bars are overlapping
  // we're going to move the bars manually if they overlap, so we want to force the bar width
  // https://github.com/recharts/recharts/blob/master/src/util/ChartUtils.ts#L1290-L1297
  // @ts-expect-error abusing recharts' internals
  axisScale.bandwidth = function scaleBarWidth(this: typeof axisScale) {
    const [left, right] = this.range();
    // unfortunately there is now way to access the number of bars in the chart from here,
    // there's two options: 8 and 30, so we'll just use 30 which gives a size that works in both cases
    const width = right - left - 120; // 120 gives ~2px padding on each side of every bar
    return Math.floor(Math.min(MAX_BAR_WIDTH, width / 30));
  };

  return {
    type: "number",
    domain: ["auto", "auto"],
    padding: { left: 25, right: 25 },
    tickCount: 2,
    tickFormatter: (x) => new Date(x).toLocaleDateString(undefined, { month: "short", day: "numeric" }),
    interval: "preserveStartEnd",
    style: { fontSize: "10px" },
    stroke: styles.labelColor,
    scale: axisScale,
  };
};

export const tooltipConfig: TooltipProps<number, string> = {
  animationDuration: 300,
  animationEasing: "ease-out",
  allowEscapeViewBox: { x: false, y: true },
  offset: 0,
};

interface ChartTick {
  coordinate: number;
}
interface ChartBar {
  x: number;
  width: number;
}
const isDataChartTicks = (x: ChartTick[] | ChartBar[]): x is ChartTick[] => {
  return x.length > 0 && "coordinate" in x[0];
};
export const adjustPositions = (_data: ChartTick[] | ChartBar[], xAxisDefinition: BaseAxisProps) => {
  const data = _data as unknown as Array<Record<string, number>>; // a lie but matches the three values we access
  const areDataTicks = isDataChartTicks(_data);
  const itemPositionKey = areDataTicks ? "coordinate" : "x";

  // @ts-expect-error `scaleLinear` doesn't have a bandwidth method, but we added it above
  const barWidth = xAxisDefinition.scale.bandwidth();

  // if the items are too close together, adjust the x/coordinate of the next bar to avoid overlapping
  for (let i = 0; i < data.length - 1; i++) {
    const item = data[i];
    const nextItem = data[i + 1];
    if (nextItem[itemPositionKey] - item[itemPositionKey] < barWidth) {
      nextItem[itemPositionKey] = item[itemPositionKey] + barWidth + 1;
    }
  }

  // while pushing to the right, items may have been shifted off of the graph entirely
  // if so, shift them back to the left
  const domain = xAxisDefinition.domain as [number, number];
  const scale = xAxisDefinition.scale as (value: number) => number;
  const [, max] = domain;
  const maxCoordinate = scale(max);

  // these two loops can probably be combined but thinking it through
  // hurts my head and there's only a handful of items in the array

  // if any item is off the right edge of the graph move it to the end
  for (let i = 0; i < data.length; i++) {
    const item = data[i];
    if (areDataTicks) {
      if (item.coordinate > maxCoordinate) {
        item.coordinate = maxCoordinate;
      }
    } else if (item.x + item.width / 2 > maxCoordinate) {
      item.x = maxCoordinate - item.width / 2;
    }
  }

  // resolve any resulting collisions
  for (let i = data.length - 1; i > 0; i--) {
    const item = data[i];
    const prevItem = data[i - 1];
    if (item[itemPositionKey] - prevItem[itemPositionKey] < barWidth) {
      prevItem[itemPositionKey] = item[itemPositionKey] - barWidth - 1;
    } else {
      break;
    }
  }
};

export const ClickToJob = (chartState: CategoricalChartState & { height: number }) => {
  const { offset, height } = chartState;
  const { top: offsetTop = 0, bottom: offsetBottom = 0 } = offset!;
  const availableHeight = height - offsetTop - offsetBottom;
  const { openModal } = useModalService();

  const connection = useCurrentConnection();

  const jobId = chartState.activePayload?.at(0)?.payload?.jobId;

  if (!jobId) {
    return null;
  }

  const handleOpenLogs = () =>
    openJobLogsModal({
      openModal,
      jobId,
      connectionId: connection.connectionId,
      connectionName: connection.name,
    });

  return (
    <rect
      x={0}
      y={offsetTop}
      width="100%"
      height={availableHeight}
      fill="transparent"
      data-testid="streams-graph-to-job-logs-modal"
      onClick={handleOpenLogs}
    />
  );
};
ClickToJob.displayName = "Customized"; // must be this to be rendered, via https://github.com/recharts/recharts/blob/e4dd3710642428692e2ebda5a51c0174f3a6f361/src/chart/generateCategoricalChart.tsx#L2336
