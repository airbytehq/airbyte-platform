import React, { useState } from "react";
import { useIntl } from "react-intl";
import { Navigate, useLocation, useNavigate, useParams } from "react-router-dom";

import { MainPageWithScroll } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectionBlock } from "components/connection/ConnectionBlock";
import { SelectDestination } from "components/connection/CreateConnection/SelectDestination";
import { SelectSource } from "components/connection/CreateConnection/SelectSource";
import { CreateConnectionForm } from "components/connection/CreateConnectionForm";
import { FormPageContent } from "components/ConnectorBlocks";
import { NextPageHeaderWithNavigation } from "components/ui/PageHeader/NextPageHeaderWithNavigation";
import { StepsIndicator } from "components/ui/StepsIndicator";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { RoutePaths } from "pages/routePaths";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import { hasDestinationId, hasSourceId, usePreloadData } from "./usePreloadData";

enum StepsTypes {
  CREATE_SOURCE = "source",
  CREATE_DESTINATION = "destination",
  CREATE_CONNECTION = "connection",
}

export const CreateConnectionPage: React.FC = () => {
  const params = useParams<{ workspaceId: string }>();

  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW);
  const location = useLocation();
  const { formatMessage } = useIntl();
  const workspaceId = useCurrentWorkspaceId();
  const { trackAction } = useAppMonitoringService();

  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Connections}`;

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.connections" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: formatMessage({ id: "connection.newConnection" }) },
  ];

  const [currentStep, setCurrentStep] = useState(
    hasSourceId(location.state) && hasDestinationId(location.state)
      ? StepsTypes.CREATE_CONNECTION
      : hasSourceId(location.state) && !hasDestinationId(location.state)
      ? StepsTypes.CREATE_DESTINATION
      : StepsTypes.CREATE_SOURCE
  );

  const { destinationDefinition, sourceDefinition, source, destination } = usePreloadData();

  const onSelectExistingSource = (id: string) => {
    clearAllFormChanges();
    navigate("", {
      state: {
        ...(location.state as Record<string, unknown>),
        sourceId: id,
      },
    });
    if (!destination) {
      setCurrentStep(StepsTypes.CREATE_DESTINATION);
    } else {
      setCurrentStep(StepsTypes.CREATE_CONNECTION);
    }
  };

  const onSelectExistingDestination = (id: string) => {
    clearAllFormChanges();
    navigate("", {
      state: {
        ...(location.state as Record<string, unknown>),
        destinationId: id,
      },
    });
    if (!source) {
      setCurrentStep(StepsTypes.CREATE_SOURCE);
    } else {
      setCurrentStep(StepsTypes.CREATE_CONNECTION);
    }
  };

  const renderStep = () => {
    if (currentStep === StepsTypes.CREATE_SOURCE) {
      return <SelectSource onSelectSource={onSelectExistingSource} />;
    } else if (currentStep === StepsTypes.CREATE_DESTINATION) {
      return <SelectDestination onSelectDestination={onSelectExistingDestination} />;
    } else if (currentStep === StepsTypes.CREATE_CONNECTION && source && destination) {
      return <CreateConnectionForm source={source} destination={destination} />;
    }
    trackAction(AppActionCodes.UNEXPECTED_CONNECTION_FLOW_STATE, {
      currentStep,
      sourceId: source?.sourceId,
      destinationId: destination?.destinationId,
      workspaceId,
    });
    return (
      <Navigate to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connection}/${RoutePaths.ConnectionNew}`} />
    );
  };

  const steps = [
    {
      id: StepsTypes.CREATE_SOURCE,
      name: formatMessage({ id: "onboarding.createSource" }),
    },
    {
      id: StepsTypes.CREATE_DESTINATION,
      name: formatMessage({ id: "onboarding.createDestination" }),
    },
    {
      id: StepsTypes.CREATE_CONNECTION,
      name: formatMessage({ id: "onboarding.setUpConnection" }),
    },
  ];

  if (currentStep === StepsTypes.CREATE_CONNECTION) {
    return (
      <MainPageWithScroll
        headTitle={<HeadTitle titles={[{ id: "connection.newConnectionTitle" }]} />}
        pageTitle={<NextPageHeaderWithNavigation breadcrumbsData={breadcrumbsData} />}
      >
        <StepsIndicator steps={steps} activeStep={currentStep} />
        {renderStep()}
      </MainPageWithScroll>
    );
  }

  return (
    <>
      <HeadTitle titles={[{ id: "connection.newConnectionTitle" }]} />
      <ConnectorDocumentationWrapper>
        <NextPageHeaderWithNavigation breadcrumbsData={breadcrumbsData} />
        <FormPageContent>
          <StepsIndicator steps={steps} activeStep={currentStep} />
          {(!!source || !!destination) && (
            <ConnectionBlock
              itemFrom={source ? { name: source.name, icon: sourceDefinition?.icon } : undefined}
              itemTo={
                destination
                  ? {
                      name: destination.name,
                      icon: destinationDefinition?.icon,
                    }
                  : undefined
              }
            />
          )}

          {renderStep()}
        </FormPageContent>
      </ConnectorDocumentationWrapper>
    </>
  );
};
