import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { useExperiment } from "hooks/services/Experiment";

import styles from "./CreditsUsage.module.scss";
import { useCreditsContext } from "./CreditsUsageContext";
import { CreditsUsageFilters } from "./CreditsUsageFilters";
import { EmptyState } from "./EmptyState";
import { UsagePerConnectionTable } from "./UsagePerConnectionTable";
import { UsagePerDayGraph } from "./UsagePerDayGraph";

const CreditsUsage: React.FC = () => {
  const { freeAndPaidUsageByTimeframe } = useCreditsContext();
  const isBillingInsightsEnabled = useExperiment("billing.billingInsights", false);

  return (
    <Card className={styles.card}>
      {isBillingInsightsEnabled && (
        <Box pt="xl">
          <CreditsUsageFilters />
        </Box>
      )}
      {freeAndPaidUsageByTimeframe.length > 0 ? (
        <>
          <Box pt="xl">
            <Box pl="lg">
              <Heading as="h5" size="sm">
                <FormattedMessage id="credits.totalCreditsUsage" />
              </Heading>
            </Box>
            <UsagePerDayGraph chartData={freeAndPaidUsageByTimeframe} />
          </Box>
          <Box pt="xl">
            <Box pl="lg">
              <Heading as="h5" size="sm">
                <FormattedMessage id="credits.usagePerConnection" />
              </Heading>
            </Box>
            <UsagePerConnectionTable />
          </Box>
        </>
      ) : (
        <EmptyState />
      )}
    </Card>
  );
};

export default CreditsUsage;
