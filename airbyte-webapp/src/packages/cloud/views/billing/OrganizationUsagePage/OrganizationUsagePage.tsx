import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { ConsumptionTimeWindow } from "core/api/types/AirbyteClient";
import { links } from "core/utils/links";
import { UsagePerDayGraph } from "packages/cloud/area/billing/components/UsagePerDayGraph";

import { OrganizationCreditUsageContextProvider, useOrganizationCreditsContext } from "./OrganizationCreditContext";
import styles from "./OrganizationUsagePage.module.scss";
import { UsageByWorkspaceTable } from "./UsageByWorkspaceTable";

export const OrganizationUsagePage: React.FC = () => {
  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer alignItems="center">
        <Heading as="h1">
          <FormattedMessage id="settings.organization.usage.title" />
        </Heading>
      </FlexContainer>

      <Text>
        <FormattedMessage
          id="settings.organization.usage.description"
          values={{
            lnk: (node: React.ReactNode) => <ExternalLink href={links.creditDescription}>{node}</ExternalLink>,
          }}
        />
      </Text>
      <OrganizationCreditUsageContextProvider>
        <TimeWindowSelector />
        <OrganizationUsageGraph />
        <UsageByWorkspaceTable />
      </OrganizationCreditUsageContextProvider>
    </FlexContainer>
  );
};

const TimeWindowSelector: React.FC = () => {
  const { formatMessage } = useIntl();
  const { selectedTimeWindow, setSelectedTimeWindow } = useOrganizationCreditsContext();
  return (
    <div className={styles.timeWindowSelector__wrapper}>
      <ListBox
        options={[
          {
            label: formatMessage({ id: "settings.organization.billing.filter.lastThirtyDays" }),
            value: ConsumptionTimeWindow.lastMonth,
          },
          {
            label: formatMessage({ id: "settings.organization.billing.filter.lastSixMonths" }),
            value: ConsumptionTimeWindow.lastSixMonths,
          },
          {
            label: formatMessage({ id: "settings.organization.billing.filter.lastTwelveMonths" }),
            value: ConsumptionTimeWindow.lastYear,
          },
        ]}
        selectedValue={selectedTimeWindow}
        onSelect={(selectedValue) => setSelectedTimeWindow(selectedValue)}
      />
    </div>
  );
};

const OrganizationUsageGraph: React.FC = () => {
  const { freeAndPaidUsageByTimeChunk, hasFreeUsage, hasInternalUsage } = useOrganizationCreditsContext();

  return (
    <UsagePerDayGraph
      hasFreeUsage={hasFreeUsage}
      hasInternalUsage={hasInternalUsage}
      chartData={freeAndPaidUsageByTimeChunk}
    />
  );
};
