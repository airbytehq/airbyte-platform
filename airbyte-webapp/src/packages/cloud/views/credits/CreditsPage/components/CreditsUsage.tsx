import React from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";

import { UsagePerDayGraph } from "./UsagePerDayGraph";

// const LegendLabels = ["value"];
// import {Mock}

const CreditsUsage: React.FC = () => {
  // const isBillingInsightsEnabled = useExperiment("billing.billingInsights", false);

  // const { formatMessage, formatDate } = useIntl();

  // todo: the following (through the return statement) can be removed once billingInsights is enabled:
  // const { workspaceId } = useCurrentWorkspace();
  // const data = useGetCloudWorkspaceUsage(workspaceId);

  // const chartData = useMemo(
  //   () =>
  //     data?.creditConsumptionByDay?.map(({ creditsConsumed, date }) => ({
  //       name: formatDate(new Date(date[0], date[1] - 1 /* zero-indexed */, date[2]), {
  //         month: "short",
  //         day: "numeric",
  //       }),
  //       value: creditsConsumed,
  //     })),
  //   [data, formatDate]
  // );

  return (
    <>
      <Card title={<FormattedMessage id="credits.totalUsage" />} lightPadding>
        {/* {isBillingInsightsEnabled ? ( */}
        <UsagePerDayGraph />
        {/* // ) : (
        //   <div className={styles.chartWrapper}>
        //     {data?.creditConsumptionByDay?.length ? (
        //       <BarChart
        //         data={chartData}
        //         legendLabels={LegendLabels}
        //         xLabel={formatMessage({
        //           id: "credits.date",
        //         })}
        //         yLabel={formatMessage({
        //           id: "credits.amount",
        //         })}
        //       />
        //     ) : (
        //       <FlexContainer alignItems="center" justifyContent="center" className={styles.empty}>
        //         <FormattedMessage id="credits.noData" />
        //       </FlexContainer>
        //     )}
        //   </div>
        // )} */}
      </Card>

      {/* <Card title={<FormattedMessage id="credits.usagePerConnection" />} lightPadding className={styles.cardBlock}>
        <UsagePerConnectionTable />
      </Card> */}
    </>
  );
};

export default CreditsUsage;
