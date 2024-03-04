import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet, useParams } from "react-router-dom";

import { LoadingPage } from "components";
import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorNavigationTabs } from "components/connector/ConnectorNavigationTabs";
import { ConnectorTitleBlock } from "components/connector/ConnectorTitleBlock";
import { StepsTypes } from "components/ConnectorBlocks";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { useGetDestinationFromParams } from "area/connector/utils";
import { useDestinationDefinitionVersion, useDestinationDefinition } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { RoutePaths } from "pages/routePaths";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

export const DestinationItemPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.DESTINATION_ITEM);
  const params = useParams<{ workspaceId: string; "*": StepsTypes | "" | undefined }>();
  const destination = useGetDestinationFromParams();
  const destinationDefinition = useDestinationDefinition(destination.destinationDefinitionId);
  const actorDefinitionVersion = useDestinationDefinitionVersion(destination.destinationId);
  const { formatMessage } = useIntl();

  const { trackError } = useAppMonitoringService();

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Destination}`;

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.destinations" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: destination.name },
  ];

  return (
    <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
      <ConnectorDocumentationWrapper>
        <HeadTitle titles={[{ id: "admin.destinations" }, { title: destination.name }]} />
        <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
          <ConnectorTitleBlock
            connector={destination}
            connectorDefinition={destinationDefinition}
            actorDefinitionVersion={actorDefinitionVersion}
          />
          <ConnectorNavigationTabs connectorType="destination" connector={destination} id={destination.destinationId} />
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
