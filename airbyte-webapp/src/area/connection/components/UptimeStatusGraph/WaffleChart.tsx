import { useLayoutEffect, useState } from "react";
import { BaseAxisProps, ChartOffset } from "recharts/types/util/types";

import { StreamStatusType } from "components/connection/StreamStatusIndicator";

export interface ChartStream {
  streamNamespace?: string;
  streamName: string;
  status: StreamStatusType;
}
export interface UptimeDayEntry {
  date: number;
  jobId: number;
  runtimeMs: number;
  recordsEmitted: number;
  recordsCommitted: number;
  streams: ChartStream[];
}

const CELL_VERTICAL_GAP = 0.25;

// these props come from us
interface StreamWaffleChartProps {
  streamsCount: number;
  colorMap: Record<string, string>;
  dataKey: string; // necessary to enable tooltip display
  maxStreamsCount: number; // max number of streams synced in a single job
}

// these are injected by recharts
interface InjectedStreamWaffleChartProps extends StreamWaffleChartProps {
  data: UptimeDayEntry[];
  colorMap: Record<string, string>;
  width: number;
  height: number;
  offset: ChartOffset;
  orderedTooltipTicks: Array<{ coordinate: number }>;
  activeTooltipIndex: number;
  isTooltipActive: boolean;
  xAxisMap: Record<string, BaseAxisProps>;
}

type WaffleColor = "green" | "red" | "yellow" | "blue" | "empty";
const getCellColor = (streamStatus: StreamStatusType): WaffleColor => {
  switch (streamStatus) {
    case StreamStatusType.Synced:
      return "green";

    case StreamStatusType.Incomplete:
      return "yellow";

    case StreamStatusType.Failed:
      return "red";

    case StreamStatusType.Queued:
    case StreamStatusType.Syncing:
    case StreamStatusType.Refreshing:
    case StreamStatusType.RateLimited:
      return "blue";

    case StreamStatusType.Pending:
    case StreamStatusType.Paused:
    case StreamStatusType.QueuedForNextSync:
    case StreamStatusType.Clearing:
      return "empty";
  }
};

export const Waffle: React.FC<StreamWaffleChartProps> = (props) => {
  const [canvas, setCanvas] = useState<HTMLCanvasElement | null>();

  const {
    data,
    colorMap,
    maxStreamsCount,
    width,
    height,
    offset,
    orderedTooltipTicks,
    activeTooltipIndex,
    isTooltipActive,
    streamsCount,
    xAxisMap,
  } = props as InjectedStreamWaffleChartProps;

  const { top: offsetTop = 0, right: offsetRight, bottom: offsetBottom = 0, left: offsetLeft } = offset;

  // @ts-expect-error `scaleLinear` doesn't have a bandwidth method, but we gave it one
  const barWidth = xAxisMap["0"]?.scale?.bandwidth?.();

  // layout effect so drawing happens immediately, prevents flicker when resizing the window
  useLayoutEffect(() => {
    if (canvas) {
      const availableHeight = height - offsetTop - offsetBottom;
      const halfBarWidth = barWidth / 2;
      const cellHeight = availableHeight / streamsCount;

      interface CellOperation {
        x: number;
        y: number;
        width: number;
        height: number;
        fillStyle: string;
      }

      const computeCellOperation = (
        columnIndex: number,
        rowIndex: number,
        status: StreamStatusType,
        skipRecurse = false
      ): CellOperation | null => {
        const cellOffset = rowIndex * cellHeight;

        if (columnIndex >= orderedTooltipTicks.length) {
          return null;
        }

        const xCoordinate = orderedTooltipTicks[columnIndex].coordinate;

        const myOperation = {
          fillStyle: colorMap[getCellColor(status)],

          // floor and ceil to avoid subpixel antialiasing & the background color bleeding through
          x: Math.floor(xCoordinate - halfBarWidth),
          y: Math.floor(offsetTop + cellOffset + CELL_VERTICAL_GAP),
          width: Math.ceil(barWidth),
          height: Math.ceil(cellHeight) - CELL_VERTICAL_GAP * 2,
        };

        // if we are not currently in a correction operation (via skipRecurse) then
        // address any extra gap that may have been introduced by subpixel positioning
        // by overdrawing the amount of the gap
        if (!skipRecurse) {
          // vertical correction
          if (CELL_VERTICAL_GAP > 0 && rowIndex < streamsCount - 1) {
            const siblingOperationY = computeCellOperation(columnIndex, rowIndex + 1, status, true);
            if (!siblingOperationY) {
              return null;
            }

            const gapY = siblingOperationY.y - (myOperation.y + myOperation.height);
            const extraGapY = CELL_VERTICAL_GAP - gapY;
            myOperation.height -= extraGapY;
          }
        }

        return myOperation;
      };

      const ctx = canvas.getContext("2d");
      if (ctx) {
        ctx.clearRect(0, 0, width, height);

        // When there are a lot of streams (somewhere between 200-250) a stream
        // is crowded out and hidden by the next stream's rendering.
        // This chart is particularly useful to indicate non-success states
        // so let's draw all the successes first and then draw the others on top
        // so the failure modes crowd out the successes.
        const ontimeOperations: CellOperation[] = [];
        const otherOperations: CellOperation[] = [];

        for (let i = 0; i < data.length; i++) {
          const { streams } = data[i];

          // with no offset, the streams are top-aligned to the graph
          // but we want them anchored to x-axis
          const rowOffset = maxStreamsCount - streams.length;

          for (let j = 0; j < streams.length; j++) {
            const { status } = streams[j];

            const operation = computeCellOperation(i, rowOffset + j, status);

            if (!operation) {
              continue;
            }

            if (status === StreamStatusType.Synced) {
              ontimeOperations.push(operation);
            } else {
              otherOperations.push(operation);
            }
          }
        }

        const allOperations = [...ontimeOperations, ...otherOperations];
        for (let i = 0; i < allOperations.length; i++) {
          const operation = allOperations[i];
          ctx.fillStyle = operation.fillStyle;
          ctx.fillRect(operation.x, operation.y, operation.width, operation.height);
        }

        // tooltip highlight
        if (isTooltipActive && activeTooltipIndex >= 0) {
          const coordinates = computeCellOperation(activeTooltipIndex, 0, StreamStatusType.Synced);
          if (coordinates) {
            ctx.fillStyle = "rgba(255, 255, 255, 0.4)";
            ctx.fillRect(coordinates.x, coordinates.y, coordinates.width, availableHeight);
            ctx.restore();
          }
        }
      }
    }
  }, [
    canvas,
    width,
    height,
    offsetTop,
    offsetRight,
    offsetBottom,
    offsetLeft,
    data,
    colorMap,
    streamsCount,
    orderedTooltipTicks,
    activeTooltipIndex,
    isTooltipActive,
    maxStreamsCount,
    barWidth,
  ]);

  return (
    <foreignObject x="0" y="0" width="100%" height="100%">
      <canvas height={height} width={width} ref={setCanvas} />
    </foreignObject>
  );
};
Waffle.displayName = "Customized"; // must be this to be rendered, via https://github.com/recharts/recharts/blob/e4dd3710642428692e2ebda5a51c0174f3a6f361/src/chart/generateCategoricalChart.tsx#L2336
