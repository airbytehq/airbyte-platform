import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet, useNavigate } from "react-router-dom";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorNavigationTabs } from "components/connector/ConnectorNavigationTabs";
import { ItemTabs, StepsTypes } from "components/ConnectorBlocks";
import LoadingPage from "components/LoadingPage";
import { Breadcrumbs } from "components/ui/Breadcrumbs";
import { PageHeader } from "components/ui/PageHeader";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperiment } from "hooks/services/Experiment";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import { useGetSourceFromParams, useGetSourceTabFromParams } from "../SourceOverviewPage/useGetSourceFromParams";

export const SourceItemPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SOURCE_ITEM);
  const source = useGetSourceFromParams();

  // from here to the below comment can be removed when flag for new connection flow is on
  const navigate = useNavigate();
  const { formatMessage } = useIntl();

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
        <PageHeader
          title={<Breadcrumbs data={breadcrumbsData} />}
          middleComponent={
            !isNewConnectionFlowEnabled && <ItemTabs currentStep={currentStep} setCurrentStep={onSelectStep} />
          }
        />
        {isNewConnectionFlowEnabled && <ConnectorNavigationTabs connectorType="source" />}
        <Suspense fallback={<LoadingPage />}>
          <ApiErrorBoundary>
            <Outlet />
          </ApiErrorBoundary>
        </Suspense>
      </ConnectorDocumentationWrapper>
    </ResourceNotFoundErrorBoundary>
  );
};
