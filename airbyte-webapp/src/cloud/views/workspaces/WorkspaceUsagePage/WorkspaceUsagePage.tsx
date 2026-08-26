import { Suspense } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { LoadingPage } from "components/ui/LoadingPage";
import { Text } from "components/ui/Text";

import { SetupBillingAlertsLink } from "area/organization/components/SetupBillingAlertsLink";
import { UsagePerDayGraph } from "cloud/area/billing/components/UsagePerDayGraph";
import { useCurrentWorkspace, useGetDataplaneGroup } from "core/api";
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
  const { dataplaneGroupId } = useCurrentWorkspace();
  const hasDataWorkerEntitlement = useFeature(FeatureItem.AllowDataWorkerCapacity);
  const isWorkspaceDwUsageEnabled = useExperiment("platform.workspace-dw-usage");
  const showDataWorkerUsage = hasDataWorkerEntitlement && isWorkspaceDwUsageEnabled;

  return (
    <FlexContainer direction="column" gap="xl">
      {showDataWorkerUsage ? (
        <>
          <FlexContainer direction="column" gap="md">
            <Heading as="h1">
              <FormattedMessage id="settings.workspace.usage.title" />
            </Heading>
            {dataplaneGroupId && (
              <Suspense fallback={null}>
                <WorkspaceRegion dataplaneGroupId={dataplaneGroupId} />
              </Suspense>
            )}
            <Text>
              <FormattedMessage id="settings.workspace.usage.dataWorker.description" />
            </Text>
          </FlexContainer>
          <Suspense fallback={<LoadingPage />}>
            <WorkspaceDataWorkerUsageGraph />
          </Suspense>
        </>
      ) : (
        <>
          <FlexContainer justifyContent="space-between" alignItems="center">
            <Heading as="h1">
              <FormattedMessage id="settings.workspace.usage.title" />
            </Heading>
            <SetupBillingAlertsLink />
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
        </>
      )}
    </FlexContainer>
  );
};

const WorkspaceRegion: React.FC<{ dataplaneGroupId: string }> = ({ dataplaneGroupId }) => {
  const { getDataplaneGroup } = useGetDataplaneGroup();
  const regionName = getDataplaneGroup(dataplaneGroupId)?.name;

  if (!regionName) {
    return null;
  }

  return (
    <FlexContainer alignItems="center" gap="sm">
      <Icon type="globe" size="sm" color="disabled" />
      <Text color="grey" size="lg">
        <FormattedMessage id="settings.workspace.usage.region" values={{ regionName }} />
      </Text>
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
