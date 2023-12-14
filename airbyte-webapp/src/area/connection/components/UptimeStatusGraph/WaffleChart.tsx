import { useEffect, useState } from "react";
import { ChartOffset } from "recharts/types/util/types";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";

// Rough idea for the data structure we'll get from API
export interface ChartStream {
  streamNamespace?: string;
  streamName: string;
  status: ConnectionStatusIndicatorStatus;
}
export interface UptimeDayEntry {
  date: number;
  streams: ChartStream[];
}

const CELL_HORIZONTAL_GAP = 2;
const CELL_VERTICAL_GAP = 0.25;

// these props come from us
interface StreamWaffleChartProps {
  streamsCount: number;
  colorMap: Record<string, string>;
  dataKey: string; // necessary to enable tooltip display
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
}

type WaffleColor = "green" | "darkBlue" | "red" | "black" | "empty";
const getCellColor = (streamStatus: ConnectionStatusIndicatorStatus): WaffleColor => {
  switch (streamStatus) {
    case ConnectionStatusIndicatorStatus.OnTime:
    case ConnectionStatusIndicatorStatus.OnTrack:
      return "green";

    case ConnectionStatusIndicatorStatus.Late:
      return "darkBlue";

    case ConnectionStatusIndicatorStatus.Error:
      return "red";

    case ConnectionStatusIndicatorStatus.ActionRequired:
      return "black";

    case ConnectionStatusIndicatorStatus.Disabled:
    case ConnectionStatusIndicatorStatus.Pending:
      return "empty";
  }
};

export const Waffle: React.FC<StreamWaffleChartProps> = (props) => {
  const [canvas, setCanvas] = useState<HTMLCanvasElement | null>();

  const {
    data,
    colorMap,
    width,
    height,
    offset,
    orderedTooltipTicks,
    activeTooltipIndex,
    isTooltipActive,
    streamsCount,
  } = props as InjectedStreamWaffleChartProps;

  const { top: offsetTop = 0, right: offsetRight, bottom: offsetBottom = 0, left: offsetLeft } = offset;

  useEffect(() => {
    if (canvas) {
      const availableHeight = height - offsetTop - offsetBottom;
      const barWidth = width / data.length - CELL_HORIZONTAL_GAP * 2;
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
        status: ConnectionStatusIndicatorStatus,
        skipRecurse = false
      ): CellOperation => {
        const cellOffset = rowIndex * cellHeight;
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
          // horizontal correction
          if (CELL_HORIZONTAL_GAP > 0 && columnIndex < data.length - 1) {
            const siblingOperationX = computeCellOperation(columnIndex + 1, rowIndex, status, true);
            const gapX = siblingOperationX.x - (myOperation.x + myOperation.width);
            const extraGapX = CELL_HORIZONTAL_GAP - gapX;
            myOperation.width -= extraGapX;
          }

          // vertical correction
          if (CELL_VERTICAL_GAP > 0 && rowIndex < streamsCount - 1) {
            const siblingOperationY = computeCellOperation(columnIndex, rowIndex + 1, status, true);
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

          for (let j = 0; j < streams.length; j++) {
            const { status } = streams[j];

            const operation = computeCellOperation(i, j, status);
            if (status === ConnectionStatusIndicatorStatus.OnTime) {
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
          const coordinates = computeCellOperation(activeTooltipIndex, 0, ConnectionStatusIndicatorStatus.OnTime);
          ctx.fillStyle = "rgba(255, 255, 255, 0.4)";
          ctx.fillRect(coordinates.x, coordinates.y, coordinates.width, availableHeight);
          ctx.restore();
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
  ]);

  return (
    <foreignObject x="0" y="0" width="100%" height="100%">
      <canvas height={height} width={width} ref={setCanvas} />
    </foreignObject>
  );
};
Waffle.displayName = "Customized"; // must be this to be rendered, via https://github.com/recharts/recharts/blob/e4dd3710642428692e2ebda5a51c0174f3a6f361/src/chart/generateCategoricalChart.tsx#L2336
