import classnames from "classnames";
import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { Text } from "components/ui/Text";

import { UsagePerTimeChunk } from "./calculateUsageDataObjects";
import { FormattedCredits } from "./FormattedCredits";
import styles from "./UsagePerDayGraph.module.scss";

interface UsagePerDayGraphProps {
  chartData: UsagePerTimeChunk;
  minimized?: boolean;
  hasFreeUsage?: boolean;
}
export const UsagePerDayGraph: React.FC<UsagePerDayGraphProps> = ({ chartData, minimized, hasFreeUsage }) => {
  const { formatMessage } = useIntl();
  const chartLinesColor = styles.grey100;
  const chartTicksColor = styles.grey;
  const chartHoverFill = styles.grey100;

  const width = useMemo(() => {
    if (chartData.length === 0) {
      return;
    }

    if (minimized) {
      return 10;
    }

    return Math.min(
      Math.max([...chartData].sort((a, b) => b.freeUsage - a.freeUsage)[0].freeUsage.toFixed(0).length * 10, 80),
      130
    );
  }, [chartData, minimized]);

  return (
    <div className={classnames(styles.container, { [styles["container--full"]]: !minimized })}>
      <ResponsiveContainer width={minimized ? 100 : undefined} height={minimized ? 30 : undefined}>
        <BarChart data={chartData} margin={minimized ? {} : { right: 12, top: 25 }}>
          {!minimized && (
            <Legend
              verticalAlign="top"
              align="right"
              iconType="circle"
              height={40}
              wrapperStyle={{ color: `${styles.white}` }}
              formatter={(value: "freeUsage" | "billedCost") => {
                return (
                  <Text as="span">
                    <FormattedMessage id={`credits.${value}`} />
                  </Text>
                );
              }}
            />
          )}
          {!minimized && <CartesianGrid vertical={false} stroke={chartLinesColor} />}
          <XAxis
            dataKey="timeChunkLabel"
            axisLine={false}
            tickLine={false}
            stroke={chartTicksColor}
            tick={{ fontSize: "11px" }}
            tickSize={7}
            hide={minimized}
          />

          {minimized && <ReferenceLine y={0} stroke={chartLinesColor} />}
          <YAxis
            axisLine={false}
            tickLine={false}
            stroke={chartTicksColor}
            tick={{ fontSize: "11px" }}
            tickSize={10}
            width={width}
            hide={minimized}
          />
          {!minimized && (
            <Tooltip
              cursor={{ fill: chartHoverFill }}
              wrapperStyle={{ outline: "none" }}
              wrapperClassName={styles.tooltipWrapper}
              formatter={() => {
                // effectively disable this formatter; the labelFormatter is used instead to render the tooltip content

                // The type cast is unfortunately necessary, due to broken typing in recharts.
                // https://github.com/recharts/recharts/issues/3008
                return [null, null] as unknown as [string, string];
              }}
              labelFormatter={(timeChunkLabel, items) => {
                // preferring `labelFormatter` instead of `content` as the latter removes all pre-defined tooltip styling
                const isLastDay = timeChunkLabel === chartData.at(-1)?.timeChunkLabel;
                return (
                  <div>
                    <Text size="lg">{timeChunkLabel}</Text>
                    {items.map(({ name, value }) => (
                      <Text color={name === "freeUsage" ? "green" : "darkBlue"} size="lg">
                        {formatMessage({
                          id: `credits.${name}`,
                        })}
                        :{" "}
                        <FormattedCredits
                          color={name === "freeUsage" ? "green" : undefined}
                          credits={value ? parseFloat(value) : 0}
                          size="md"
                        />
                      </Text>
                    ))}
                    {isLastDay && (
                      <Text className={styles.recentCreditUsage}>
                        <FormattedMessage id="credits.l24HourCredits" />
                      </Text>
                    )}
                  </div>
                );
              }}
            />
          )}
          <Bar key="paid" stackId="a" dataKey="billedCost" fill={styles.darkBlue} isAnimationActive={!minimized}>
            {chartData.map((item, index) => {
              return (
                <Cell
                  key={`cell-paid-${index}`}
                  // recharts takes an array here, but their types only permit a string or number :/
                  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                  // @ts-ignore-next-line
                  radius={
                    item.freeUsage && item.freeUsage / (item.freeUsage + item.billedCost) > 0.01 ? 0 : [4, 4, 0, 0]
                  }
                />
              );
            })}
          </Bar>
          {(hasFreeUsage || minimized) && (
            <Bar
              key="free"
              stackId="a"
              dataKey="freeUsage"
              fill={styles.green}
              radius={[4, 4, 0, 0]}
              isAnimationActive={!minimized}
            >
              {chartData.map((item, index) => {
                return item.freeUsage && item.freeUsage / (item.freeUsage + item.billedCost) < 0.01 ? (
                  <Cell key={`cell-free-${index}`} width={0} />
                ) : (
                  <Cell key={`cell-free-${index}`} />
                );
              })}
            </Bar>
          )}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};
