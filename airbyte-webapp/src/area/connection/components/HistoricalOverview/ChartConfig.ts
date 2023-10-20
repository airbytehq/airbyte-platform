import { TooltipProps, XAxisProps } from "recharts";

import styles from "./ChartConfig.module.scss";

export const xAxisConfig: XAxisProps = {
  type: "number",
  domain: ["dataMin - 43200000", "dataMax + 43200000"], // add 12 hours to each side of the chart to avoid trimming the first and last column widths
  tickCount: 6,
  tickFormatter: (x) => new Date(x).toLocaleDateString(undefined, { month: "short", day: "numeric" }),
  interval: "preserveStartEnd",
  style: { fontSize: "10px" },
  stroke: styles.labelColor,
};

export const tooltipConfig: TooltipProps<number, string> = {
  animationDuration: 300,
  animationEasing: "ease-out",
  allowEscapeViewBox: { x: false, y: true },
  offset: 0,
};
