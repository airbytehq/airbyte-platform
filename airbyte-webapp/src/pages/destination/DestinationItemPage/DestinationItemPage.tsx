import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet, useNavigate } from "react-router-dom";

import { LoadingPage } from "components";
import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorNavigationTabs } from "components/connector/ConnectorNavigationTabs";
import { ItemTabs, StepsTypes } from "components/ConnectorBlocks";
import { Breadcrumbs } from "components/ui/Breadcrumbs";
import { PageHeader } from "components/ui/PageHeader";

import { useTrackPage, PageTrackingCodes } from "hooks/services/Analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperiment } from "hooks/services/Experiment";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import { useGetDestinationFromParams, useGetDestinationTabFromParams } from "../useGetDestinationFromParams";

export const DestinationItemPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.DESTINATION_ITEM);
  const destination = useGetDestinationFromParams();

  const { formatMessage } = useIntl();
  const isNewConnectionFlowEnabled = useExperiment("connection.updatedConnectionFlow", false);
  const currentStep = useGetDestinationTabFromParams();

  const { trackError } = useAppMonitoringService();

  // can be removed after flag enabled for all users
  const navigate = useNavigate();
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
        <PageHeader
          title={<Breadcrumbs data={breadcrumbsData} />}
          middleComponent={
            !isNewConnectionFlowEnabled && <ItemTabs currentStep={currentStep} setCurrentStep={onSelectStep} />
          }
        />
        {isNewConnectionFlowEnabled && <ConnectorNavigationTabs connectorType="destination" />}

        <Suspense fallback={<LoadingPage />}>
          <ApiErrorBoundary>
            <Outlet />
          </ApiErrorBoundary>
        </Suspense>
      </ConnectorDocumentationWrapper>
    </ResourceNotFoundErrorBoundary>
  );
};
