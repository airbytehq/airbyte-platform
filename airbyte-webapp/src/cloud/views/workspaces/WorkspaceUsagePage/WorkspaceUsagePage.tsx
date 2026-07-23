import { Suspense } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { LoadingPage } from "components/ui/LoadingPage";
import { Text } from "components/ui/Text";

import { SetupBillingAlertsLink } from "area/organization/components/SetupBillingAlertsLink";
import { UsagePerDayGraph } from "cloud/area/billing/components/UsagePerDayGraph";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useExperiment } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";
import { links } from "core/utils/links";

import { useCreditsContext, WorkspaceCreditUsageContextProvider } from "./components/CreditsUsageContext";
import { CreditsUsageFilters } from "./components/CreditsUsageFilters";
import { UsagePerConnectionTable } from "./components/UsagePerConnectionTable";
import { WorkspaceDataWorkerUsageGraph } from "./components/WorkspaceDataWorkerUsageGraph";

export const WorkspaceUsagePage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE_USAGE);
  const hasDataWorkerEntitlement = useFeature(FeatureItem.AllowDataWorkerCapacity);
  const isWorkspaceDwUsageEnabled = useExperiment("platform.workspace-dw-usage");
  const showDataWorkerUsage = hasDataWorkerEntitlement && isWorkspaceDwUsageEnabled;

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer justifyContent="space-between" alignItems="center">
        <Heading as="h1">
          <FormattedMessage id="settings.workspace.usage.title" />
        </Heading>
        {!showDataWorkerUsage && <SetupBillingAlertsLink />}
      </FlexContainer>

      {showDataWorkerUsage ? (
        <>
          <Text>
            <FormattedMessage id="settings.workspace.usage.dataWorker.description" />
          </Text>
          <Suspense fallback={<LoadingPage />}>
            <WorkspaceDataWorkerUsageGraph />
          </Suspense>
        </>
      ) : (
        <>
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
        </>
      )}
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
