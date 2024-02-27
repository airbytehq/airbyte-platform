import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis } from "recharts";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useGetConnectionDataHistory } from "core/api";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import { CHART_BASE_HEIGHT, CHART_MAX_HEIGHT, CHART_MIN_HEIGHT, CHART_STREAM_ROW_HEIGHT } from "./constants";
import styles from "./DataMovedGraph.module.scss";
import { tooltipConfig, xAxisConfig } from "../HistoricalOverview/ChartConfig";
import { NoDataMessage } from "../HistoricalOverview/NoDataMessage";

export const DataMovedGraph: React.FC = () => {
  const { connection } = useConnectionEditService();
  const data = useGetConnectionDataHistory(connection.connectionId);
  const hasData = data.some(({ recordsCommitted }) => recordsCommitted > 0);
  const navigate = useNavigate();
  const createLink = useCurrentWorkspaceLink();

  const formattedData = useMemo(
    () => data.map(({ timestamp, recordsCommitted }) => ({ date: timestamp * 1000, recordsCommitted })),
    [data]
  );

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

        <Bar
          dataKey="recordsCommitted"
          fill={styles.recordsColor}
          isAnimationActive={false}
          onMouseDown={({ date }) => {
            const dateString = dayjs(date).format("YYYY-MM-DD");
            navigate({
              pathname: `${createLink(
                `/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.JobHistory}`
              )}`,
              search: `?startDate=${dateString}&endDate=${dateString}`,
            });
          }}
        />

        <Tooltip
          cursor={{ fill: styles.chartHoverFill, opacity: 0.65 }}
          labelStyle={{ color: styles.tooltipLabelColor }}
          itemStyle={{ color: styles.tooltipItemColor }}
          wrapperClassName={styles.tooltipWrapper}
          labelFormatter={(value: number) => (
            <Box pb="sm">
              <Text size="md">{new Date(value).toLocaleDateString()}</Text>
            </Box>
          )}
          formatter={(value: number) => {
            // The type cast is unfortunately necessary, due to broken typing in recharts.
            // What we return is a [string, undefined], and the library accepts this as well, but the types
            // require the first element to be of the same type as value, which isn't what the formatter
            // is supposed to do: https://github.com/recharts/recharts/issues/3008
            return [
              <>
                <Text>
                  <FormattedMessage id="connection.overview.graph.dataMoved.tooltipLabel" values={{ value }} />
                </Text>
                <Text>
                  <FormattedMessage id="connection.overview.graph.dataMoved.tooltipLabelExtra" />
                </Text>
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
