import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet, useNavigate, useParams } from "react-router-dom";

import { LoadingPage } from "components";
import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorNavigationTabs } from "components/connector/ConnectorNavigationTabs";
import { ConnectorTitleBlock } from "components/connector/ConnectorTitleBlock";
import { ItemTabs, StepsTypes } from "components/ConnectorBlocks";
import { Breadcrumbs } from "components/ui/Breadcrumbs";
import { PageHeader } from "components/ui/PageHeader";
import { NextPageHeaderWithNavigation } from "components/ui/PageHeader/NextPageHeaderWithNavigation";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperiment } from "hooks/services/Experiment";
import { RoutePaths } from "pages/routePaths";
import { useDestinationDefinition } from "services/connector/DestinationDefinitionService";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import { useGetDestinationFromParams, useGetDestinationTabFromParams } from "../useGetDestinationFromParams";

export const DestinationItemPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.DESTINATION_ITEM);
  const params = useParams<{ workspaceId: string; "*": StepsTypes | "" | undefined }>();
  const destination = useGetDestinationFromParams();
  const destinationDefinition = useDestinationDefinition(destination.destinationDefinitionId);
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const isNewConnectionFlowEnabled = useExperiment("connection.updatedConnectionFlow", false);
  const currentStep = useGetDestinationTabFromParams();

  const { trackError } = useAppMonitoringService();

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Destination}`;

  const nextBreadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.destinations" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: destination.name },
  ];

  // can be removed after flag enabled for all users
  const onSelectStep = (id: string) => {
    const path = id === StepsTypes.OVERVIEW ? "." : id.toLowerCase();
    navigate(path);
  };

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "admin.destinations" }),
      to: "..",
    },
    { label: destination.name },
  ];
  // to here

  return (
    <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
      <ConnectorDocumentationWrapper>
        <HeadTitle titles={[{ id: "admin.destinations" }, { title: destination.name }]} />
        {!isNewConnectionFlowEnabled ? (
          <PageHeader
            title={<Breadcrumbs data={breadcrumbsData} />}
            middleComponent={
              !isNewConnectionFlowEnabled && <ItemTabs currentStep={currentStep} setCurrentStep={onSelectStep} />
            }
          />
        ) : (
          <NextPageHeaderWithNavigation breadCrumbsData={nextBreadcrumbsData}>
            <ConnectorTitleBlock connector={destination} connectorDefinition={destinationDefinition} />
            <ConnectorNavigationTabs
              connectorType="destination"
              connector={destination}
              id={destination.destinationId}
            />
          </NextPageHeaderWithNavigation>
        )}
        <Suspense fallback={<LoadingPage />}>
          <ApiErrorBoundary>
            <Outlet />
          </ApiErrorBoundary>
        </Suspense>
      </ConnectorDocumentationWrapper>
    </ResourceNotFoundErrorBoundary>
  );
};
