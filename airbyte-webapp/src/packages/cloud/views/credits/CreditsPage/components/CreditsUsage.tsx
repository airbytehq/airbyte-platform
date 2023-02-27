import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { BarChart } from "components/ui/BarChart";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useExperiment } from "hooks/services/Experiment";
import { useGetCloudWorkspaceUsage } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import styles from "./CreditsUsage.module.scss";
import { UsagePerDayGraph } from "./UsagePerDayGraph";

const LegendLabels = ["value"];
// import {Mock}

const CreditsUsage: React.FC = () => {
  const isBillingInsightsEnabled = useExperiment("billing.billingInsights", false);

  const { formatMessage, formatDate } = useIntl();

  // todo: the following (through the return statement) can be removed once billingInsights is enabled:
  const { workspaceId } = useCurrentWorkspace();
  const data = useGetCloudWorkspaceUsage(workspaceId);

  const chartData = useMemo(
    () =>
      data?.creditConsumptionByDay?.map(({ creditsConsumed, date }) => ({
        name: formatDate(new Date(date[0], date[1] - 1 /* zero-indexed */, date[2]), {
          month: "short",
          day: "numeric",
        }),
        value: creditsConsumed,
      })),
    [data, formatDate]
  );

  return (
    <>
      <Card title={<FormattedMessage id="credits.totalUsage" />} lightPadding>
        {isBillingInsightsEnabled ? (
          <UsagePerDayGraph />
        ) : (
          <div className={styles.chartWrapper}>
            {data?.creditConsumptionByDay?.length ? (
              <BarChart
                data={chartData}
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
        )}
      </Card>

      {/* <Card title={<FormattedMessage id="credits.usagePerConnection" />} lightPadding className={styles.cardBlock}>
        <UsagePerConnectionTable />
      </Card> */}
    </>
  );
};

export default CreditsUsage;
