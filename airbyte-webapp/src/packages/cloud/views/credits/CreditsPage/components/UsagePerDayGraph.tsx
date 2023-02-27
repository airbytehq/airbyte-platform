import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Bar, BarChart, CartesianGrid, Label, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { theme } from "theme";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./UsagePerDayGraph.module.scss";
import { useCreditsUsage } from "./useCreditsUsage";

export const UsagePerDayGraph: React.FC = () => {
  const { freeAndPaidUsagePerDay: data } = useCreditsUsage();

  const { formatMessage } = useIntl();
  const { formatNumber } = useIntl();
  const chartLinesColor = theme.grey100;
  const chartTicksColor = theme.grey;
  const chartHoverFill = theme.grey100;

  const width = useMemo(
    () =>
      Math.min(
        Math.max([...data].sort((a, b) => b.freeUsage - a.freeUsage)[0].freeUsage.toFixed(0).length * 10, 80),
        130
      ),
    [data]
  );
  return (
    <div className={styles.container}>
      {data && data.length > 0 ? (
        <ResponsiveContainer>
          <BarChart data={data} margin={{ right: 12, top: 25 }}>
            <Legend
              verticalAlign="top"
              align="right"
              iconType="circle"
              height={40}
              wrapperStyle={{ color: "#000000" }}
              formatter={(value) => {
                return (
                  <Text as="span">
                    <FormattedMessage id={`credits.${value}`} />
                  </Text>
                );
              }}
            />
            <CartesianGrid vertical={false} stroke={chartLinesColor} />
            <XAxis
              label={
                <Label
                  value={formatMessage({
                    id: "credits.date",
                  })}
                  offset={0}
                  position="insideBottom"
                  fontSize={11}
                  fill={chartTicksColor}
                  fontWeight={600}
                />
              }
              dataKey="timeframe"
              axisLine={false}
              tickLine={false}
              stroke={chartTicksColor}
              tick={{ fontSize: "11px" }}
              tickSize={7}
            />
            <YAxis
              axisLine={false}
              tickLine={false}
              stroke={chartTicksColor}
              tick={{ fontSize: "11px" }}
              tickSize={10}
              width={width}
            >
              <Label
                value={formatMessage({
                  id: "credits.amount",
                })}
                fontSize={11}
                fill={chartTicksColor}
                fontWeight={600}
                position="top"
                offset={10}
              />
            </YAxis>
            <Tooltip
              cursor={{ fill: chartHoverFill }}
              formatter={(value: number, payload) => {
                // The type cast is unfortunately necessary, due to broken typing in recharts.
                // What we return is a [string, string], and the library accepts this as well, but the types
                // require the first element to be of the same type as value, which isn't what the formatter
                // is supposed to do: https://github.com/recharts/recharts/issues/3008

                return [
                  formatNumber(value, { maximumFractionDigits: 2, minimumFractionDigits: 2 }),
                  formatMessage({
                    id: `credits.${payload}`,
                  }),
                ] as unknown as [number, string];
              }}
            />
            <Bar key="paid" stackId="a" dataKey="billedCost" fill={styles.grey} />
            {/* {data.map((item, index) => {
              console.log(item.freeUsage);
              return <Cell key={`cell-paid-${index}`} fill={styles.green} />;
            })} */}
            <Bar key="free" stackId="a" dataKey="freeUsage" fill={styles.green} />
          </BarChart>
        </ResponsiveContainer>
      ) : (
        <FlexContainer alignItems="center" justifyContent="center" className={styles.empty}>
          <FormattedMessage id="credits.noData" />
        </FlexContainer>
      )}
    </div>
  );
};
