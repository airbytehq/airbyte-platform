import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { UsagePerDayGraph } from "cloud/area/billing/components/UsagePerDayGraph";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { links } from "core/utils/links";

import { useCreditsContext, WorkspaceCreditUsageContextProvider } from "./components/CreditsUsageContext";
import { CreditsUsageFilters } from "./components/CreditsUsageFilters";
import { UsagePerConnectionTable } from "./components/UsagePerConnectionTable";

export const WorkspaceUsagePage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE_USAGE);
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
