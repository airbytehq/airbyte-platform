import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";

import styles from "./CreditsUsage.module.scss";
import UsagePerConnectionTable from "./UsagePerConnectionTable";
import { UsagePerDayGraph } from "./UsagePerDayGraph";
import { useCreditsUsage } from "./useCreditsUsage";

const CreditsUsage: React.FC = () => {
  const { formatDate } = useIntl();
  const { data } = useCreditsUsage();

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
        <UsagePerDayGraph data={chartData} />
      </Card>

      <Card title={<FormattedMessage id="credits.usagePerConnection" />} lightPadding className={styles.cardBlock}>
        <UsagePerConnectionTable />
      </Card>
    </>
  );
};

export default CreditsUsage;
