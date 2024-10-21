import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";
import { UsagePerDayGraph } from "packages/cloud/area/billing/components/UsagePerDayGraph";

import {
  useCreditsContext,
  WorkspaceCreditUsageContextProvider,
} from "../../billing/BillingPage/components/CreditsUsageContext";
import { CreditsUsageFilters } from "../../billing/BillingPage/components/CreditsUsageFilters";
import { UsagePerConnectionTable } from "../../billing/BillingPage/components/UsagePerConnectionTable";

export const WorkspaceUsagePage: React.FC = () => {
  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer alignItems="center">
        <Heading as="h1">
          <FormattedMessage id="settings.workspace.usage.title" />
        </Heading>
      </FlexContainer>

      <Text>
        <FormattedMessage
          id="settings.workspace.usage.tooltip"
          values={{
            lnk: (node: React.ReactNode) => <ExternalLink href={links.creditDescription}>{node}</ExternalLink>,
          }}
        />
      </Text>
      <WorkspaceCreditUsageContextProvider>
        <CreditsUsageFilters />
        <WorkspaceUsageGraph />
        <WorkspaceUsagePerConnectionTable />
      </WorkspaceCreditUsageContextProvider>
    </FlexContainer>
  );
};

const WorkspaceUsageGraph: React.FC = () => {
  const { freeAndPaidUsageByTimeChunk, hasFreeUsage, hasInternalUsage } = useCreditsContext();

  return (
    <UsagePerDayGraph
      hasFreeUsage={hasFreeUsage}
      hasInternalUsage={hasInternalUsage}
      chartData={freeAndPaidUsageByTimeChunk}
    />
  );
};

const WorkspaceUsagePerConnectionTable: React.FC = () => {
  const { freeAndPaidUsageByConnection } = useCreditsContext();

  return <UsagePerConnectionTable freeAndPaidUsageByConnection={freeAndPaidUsageByConnection} />;
};
