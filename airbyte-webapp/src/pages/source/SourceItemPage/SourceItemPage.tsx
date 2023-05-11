import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet, useNavigate, useParams } from "react-router-dom";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorNavigationTabs } from "components/connector/ConnectorNavigationTabs";
import { ConnectorTitleBlock } from "components/connector/ConnectorTitleBlock";
import { ItemTabs, StepsTypes } from "components/ConnectorBlocks";
import LoadingPage from "components/LoadingPage";
import { Breadcrumbs } from "components/ui/Breadcrumbs";
import { PageHeader } from "components/ui/PageHeader";
import { NextPageHeaderWithNavigation } from "components/ui/PageHeader/NextPageHeaderWithNavigation";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperiment } from "hooks/services/Experiment";
import { RoutePaths } from "pages/routePaths";
import { useSourceDefinition } from "services/connector/SourceDefinitionService";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import { useGetSourceFromParams, useGetSourceTabFromParams } from "../SourceOverviewPage/useGetSourceFromParams";

export const SourceItemPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SOURCE_ITEM);
  const params = useParams<{ workspaceId: string; "*": StepsTypes | "" | undefined }>();
  const source = useGetSourceFromParams();
  const sourceDefinition = useSourceDefinition(source.sourceDefinitionId);
  const navigate = useNavigate();
  const { formatMessage } = useIntl();

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Source}`;

  const nextBreadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.sources" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: source.name },
  ];

  const onSelectStep = (id: string) => {
    const path = id === StepsTypes.OVERVIEW ? "." : id.toLowerCase();
    navigate(path);
  };

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "admin.sources" }),
      to: "..",
    },
    { label: source.name },
  ];
  // to here

  const isNewConnectionFlowEnabled = useExperiment("connection.updatedConnectionFlow", false);

  const currentStep = useGetSourceTabFromParams();

  const { trackError } = useAppMonitoringService();

  return (
    <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
      <ConnectorDocumentationWrapper>
        <HeadTitle titles={[{ id: "admin.sources" }, { title: source.name }]} />
        {!isNewConnectionFlowEnabled ? (
          <PageHeader
            title={<Breadcrumbs data={breadcrumbsData} />}
            middleComponent={<ItemTabs currentStep={currentStep} setCurrentStep={onSelectStep} />}
          />
        ) : (
          <NextPageHeaderWithNavigation breadCrumbsData={nextBreadcrumbsData}>
            <ConnectorTitleBlock connector={source} connectorDefinition={sourceDefinition} />
            <ConnectorNavigationTabs connectorType="source" connector={source} id={source.sourceId} />
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
