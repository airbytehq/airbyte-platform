import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { BarChart } from "components/ui/BarChart";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useExperiment } from "hooks/services/Experiment";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import { useGetCloudWorkspaceUsage } from "packages/cloud/services/workspaces/CloudWorkspacesService";

import styles from "./CreditsUsage.module.scss";
import { EmptyState } from "./EmptyState";
import { NextUsagePerConnectionTable } from "./NextUsagePerConnectionTable";
import { UsagePerConnectionTable } from "./UsagePerConnectionTable";
import { UsagePerDayGraph } from "./UsagePerDayGraph";
import { useCreditsUsage } from "./useCreditsUsage";

const LegendLabels = ["value"];

const CreditsUsage: React.FC = () => {
  const isBillingInsightsEnabled = useExperiment("billing.billingInsights", false);

  const { freeAndPaidUsageByTimeframe } = useCreditsUsage();

  const { formatMessage, formatDate } = useIntl();

  // todo: the following (through the return statement) can be removed once billingInsights is enabled
  // since the data will be fetched in the UsagePerDayGraph component

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
    <FlexContainer direction="column">
      {freeAndPaidUsageByTimeframe.length > 0 ? (
        <>
          <Card title={<FormattedMessage id="credits.totalUsage" />} lightPadding className={styles.cardBlock}>
            {isBillingInsightsEnabled ? (
              <UsagePerDayGraph chartData={freeAndPaidUsageByTimeframe} />
            ) : (
              <div className={styles.chartWrapper}>
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
              </div>
            )}
          </Card>

          <Card title={<FormattedMessage id="credits.usagePerConnection" />} lightPadding className={styles.cardBlock}>
            {isBillingInsightsEnabled ? <NextUsagePerConnectionTable /> : <UsagePerConnectionTable />}
          </Card>
        </>
      ) : (
        <EmptyState />
      )}
    </FlexContainer>
  );
};

export default CreditsUsage;
