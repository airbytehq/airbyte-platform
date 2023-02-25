import { FormattedMessage, useIntl } from "react-intl";

import { BarChart } from "components/ui/BarChart";
import { FlexContainer } from "components/ui/Flex";

import styles from "./UsagePerDayGraph.module.scss";

interface UsagePerDayGraphProps {
  data: Array<{
    name: string;
    value: number;
  }>;
}
const LegendLabels = ["value"];

export const UsagePerDayGraph: React.FC<UsagePerDayGraphProps> = ({ data }) => {
  const { formatMessage } = useIntl();

  return (
    <div className={styles.container}>
      {data && data.length > 0 ? (
        <BarChart
          data={data}
          legendLabels={LegendLabels}
          xLabel={formatMessage({
            id: "credits.date",
          })}
          yLabel={formatMessage({
            id: "credits.amount",
          })}
        />
      ) : (
        <FlexContainer alignItems="center" justifyContent="center" className={styles.empty}>
          <FormattedMessage id="credits.noData" />
        </FlexContainer>
      )}
    </div>
  );
};
