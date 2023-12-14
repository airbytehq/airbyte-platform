import byteSize from "byte-size";
import { useMemo } from "react";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis } from "recharts";

import { Box } from "components/ui/Box";

import { useGetConnectionDataHistory } from "core/api";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

import { CHART_BASE_HEIGHT, CHART_MAX_HEIGHT, CHART_MIN_HEIGHT, CHART_STREAM_ROW_HEIGHT } from "./constants";
import styles from "./DataMovedGraph.module.scss";
import { tooltipConfig, xAxisConfig } from "../HistoricalOverview/ChartConfig";
import { NoDataMessage } from "../HistoricalOverview/NoDataMessage";

export const DataMovedGraph: React.FC = () => {
  const { connection } = useConnectionEditService();
  const data = useGetConnectionDataHistory(connection.connectionId);
  const hasData = data.some(({ bytes }) => bytes > 0);

  const formattedData = useMemo(() => data.map(({ timestamp, bytes }) => ({ date: timestamp * 1000, bytes })), [data]);

  const chartHeight = Math.max(
    CHART_MIN_HEIGHT,
    Math.min(CHART_MAX_HEIGHT, connection.syncCatalog.streams.length * CHART_STREAM_ROW_HEIGHT + CHART_BASE_HEIGHT)
  );

  if (!hasData) {
    return <NoDataMessage />;
  }

  return (
    <ResponsiveContainer width="100%" height={chartHeight}>
      <BarChart data={formattedData}>
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
