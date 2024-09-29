import React from "react";
import { FormattedMessage } from "react-intl";
import { useEffectOnce } from "react-use";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { trackTiming } from "core/utils/datadog";

import styles from "./CreditsUsage.module.scss";
import { useCreditsContext } from "./CreditsUsageContext";
import { CreditsUsageFilters } from "./CreditsUsageFilters";
import { EmptyState } from "./EmptyState";
import { UsagePerConnectionTable } from "./UsagePerConnectionTable";
import { UsagePerDayGraph } from "./UsagePerDayGraph";

export const CreditsUsage: React.FC = () => {
  const { freeAndPaidUsageByTimeChunk, hasFreeUsage, freeAndPaidUsageByConnection } = useCreditsContext();

  useEffectOnce(() => {
    trackTiming("CreditUsage");
  });

  return (
    <Card className={styles.card}>
      <Box pt="xl" px="lg">
        <CreditsUsageFilters />
      </Box>
      {freeAndPaidUsageByTimeChunk.length > 0 ? (
        <>
          <Box pt="xl">
            <Box pl="lg">
              <Heading as="h5" size="sm">
                <FormattedMessage id="credits.totalCreditsUsage" />
              </Heading>
            </Box>
            <UsagePerDayGraph chartData={freeAndPaidUsageByTimeChunk} hasFreeUsage={hasFreeUsage} />
          </Box>
          <Box pt="xl">
            <Box pl="lg">
              <Heading as="h5" size="sm">
                <FormattedMessage id="credits.usagePerConnection" />
              </Heading>
            </Box>
            <UsagePerConnectionTable freeAndPaidUsageByConnection={freeAndPaidUsageByConnection} />
          </Box>
        </>
      ) : (
        <EmptyState />
      )}
    </Card>
  );
};
