import React from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import styles from "./CreditsUsage.module.scss";
import { EmptyState } from "./EmptyState";
import { UsagePerConnectionTable } from "./UsagePerConnectionTable";
import { UsagePerDayGraph } from "./UsagePerDayGraph";
import { useCreditsUsage } from "./useCreditsUsage";

const CreditsUsage: React.FC = () => {
  const { freeAndPaidUsageByTimeframe } = useCreditsUsage();

  return (
    <FlexContainer direction="column">
      {freeAndPaidUsageByTimeframe.length > 0 ? (
        <>
          <Card title={<FormattedMessage id="credits.totalUsage" />} lightPadding className={styles.cardBlock}>
            <UsagePerDayGraph chartData={freeAndPaidUsageByTimeframe} />
          </Card>

          <Card title={<FormattedMessage id="credits.usagePerConnection" />} lightPadding className={styles.cardBlock}>
            <UsagePerConnectionTable />
          </Card>
        </>
      ) : (
        <EmptyState />
      )}
    </FlexContainer>
  );
};

export default CreditsUsage;
