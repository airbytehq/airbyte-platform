import byteSize from "byte-size";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis } from "recharts";

import { Box } from "components/ui/Box";

import styles from "./DataMovedGraph.module.scss";
import { tooltipConfig, xAxisConfig } from "../HistoricalOverview/ChartConfig";

const sampleData: Array<{ date: number; bytes: number }> = [];
for (let i = 1; i <= 30; i++) {
  sampleData.push({ date: Date.UTC(2023, 7, i), bytes: Math.round(Math.random() * 1000 + 200) });
}

export const DataMovedGraph: React.FC = () => {
  return (
    <ResponsiveContainer width="100%" height={100}>
      <BarChart data={sampleData}>
        <XAxis dataKey="date" {...xAxisConfig} />

        <Bar dataKey="bytes" fill={xAxisConfig.stroke} isAnimationActive={false} />

        <Tooltip
          cursor={{ fill: styles.chartHoverFill, opacity: 0.65 }}
          labelStyle={{ color: styles.tooltipLabelColor }}
          itemStyle={{ color: styles.tooltipItemColor }}
          wrapperClassName={styles.tooltipWrapper}
          labelFormatter={(value: number) => <Box pb="sm">{new Date(value).toLocaleDateString()}</Box>}
          formatter={(value: number) => {
            // The type cast is unfortunately necessary, due to broken typing in recharts.
            // What we return is a [string, undefined], and the library accepts this as well, but the types
            // require the first element to be of the same type as value, which isn't what the formatter
            // is supposed to do: https://github.com/recharts/recharts/issues/3008
            const prettyvalues = byteSize(value);
            return [
              <>
                <strong>{prettyvalues.value}</strong> {prettyvalues.long}
              </>,
              undefined,
            ] as unknown as [number, string];
          }}
          {...tooltipConfig}
        />
      </BarChart>
    </ResponsiveContainer>
  );
};
