import React from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import styles from "./CreditsUsage.module.scss";
import { EmptyState } from "./EmptyState";
import { UsagePerConnectionTable } from "./UsagePerConnectionTable";
import { UsagePerDayGraph } from "./UsagePerDayGraph";
import { useCreditsUsage } from "./useCreditsUsage";

const CreditsUsage: React.FC = () => {
  const { freeAndPaidUsageByTimeframe } = useCreditsUsage();

  return (
    <Card className={styles.card}>
      {freeAndPaidUsageByTimeframe.length > 0 ? (
        <>
          <div className={styles.section}>
            <Heading as="h5" size="sm" className={styles.heading}>
              <FormattedMessage id="credits.totalCreditsUsage" />
            </Heading>
            <UsagePerDayGraph chartData={freeAndPaidUsageByTimeframe} />
          </div>
          <div className={styles.section}>
            <Heading as="h5" size="sm" className={styles.heading}>
              <FormattedMessage id="credits.usagePerConnection" />
            </Heading>
            <UsagePerConnectionTable />
          </div>
        </>
      ) : (
        <EmptyState />
      )}
    </Card>
  );
};

export default CreditsUsage;
