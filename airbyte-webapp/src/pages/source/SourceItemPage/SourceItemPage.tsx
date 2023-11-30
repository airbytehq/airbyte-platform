import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet, useParams } from "react-router-dom";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorNavigationTabs } from "components/connector/ConnectorNavigationTabs";
import { ConnectorTitleBlock } from "components/connector/ConnectorTitleBlock";
import { StepsTypes } from "components/ConnectorBlocks";
import LoadingPage from "components/LoadingPage";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { useGetSourceFromParams } from "area/connector/utils";
import { useSourceDefinitionVersion, useSourceDefinition } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { RoutePaths } from "pages/routePaths";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

export const SourceItemPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SOURCE_ITEM);
  const params = useParams<{ workspaceId: string; "*": StepsTypes | "" | undefined }>();
  const source = useGetSourceFromParams();
  const sourceDefinition = useSourceDefinition(source.sourceDefinitionId);
  const actorDefinitionVersion = useSourceDefinitionVersion(source.sourceId);
  const { formatMessage } = useIntl();

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Source}`;

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.sources" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: source.name },
  ];

  const { trackError } = useAppMonitoringService();

  return (
    <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
      <ConnectorDocumentationWrapper>
        <HeadTitle titles={[{ id: "admin.sources" }, { title: source.name }]} />
        <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
          <ConnectorTitleBlock
            connector={source}
            connectorDefinition={sourceDefinition}
            actorDefinitionVersion={actorDefinitionVersion}
          />
          <ConnectorNavigationTabs connectorType="source" connector={source} id={source.sourceId} />
        </PageHeaderWithNavigation>
        <Suspense fallback={<LoadingPage />}>
          <ApiErrorBoundary>
            <Outlet />
          </ApiErrorBoundary>
        </Suspense>
      </ConnectorDocumentationWrapper>
    </ResourceNotFoundErrorBoundary>
  );
};
