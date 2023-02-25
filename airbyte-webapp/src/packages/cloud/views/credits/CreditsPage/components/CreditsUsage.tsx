import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import { useGetCloudWorkspaceUsage } from "packages/cloud/services/workspaces/CloudWorkspacesService";

import styles from "./CreditsUsage.module.scss";
import UsagePerConnectionTable from "./UsagePerConnectionTable";
import { UsagePerDayGraph } from "./UsagePerDayGraph";

const CreditsUsage: React.FC = () => {
  const { formatDate } = useIntl();

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
        <UsagePerDayGraph data={chartData} />
      </Card>

      <Card title={<FormattedMessage id="credits.usagePerConnection" />} lightPadding className={styles.cardBlock}>
        {data?.creditConsumptionByConnector?.length ? (
          <UsagePerConnectionTable creditConsumption={data.creditConsumptionByConnector} />
        ) : (
          <FlexContainer alignItems="center" justifyContent="center" className={styles.empty}>
            <FormattedMessage id="credits.noData" />
          </FlexContainer>
        )}
      </Card>
    </>
  );
};

export default CreditsUsage;
