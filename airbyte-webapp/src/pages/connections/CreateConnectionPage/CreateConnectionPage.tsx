import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Navigate, useLocation, useNavigate } from "react-router-dom";

import { MainPageWithScroll } from "components";
import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectionBlock } from "components/connection/ConnectionBlock";
import { SelectDestination } from "components/connection/CreateConnection/SelectDestination";
import { SelectSource } from "components/connection/CreateConnection/SelectSource";
import { CreateConnectionForm } from "components/connection/CreateConnectionForm";
import { FormPageContent } from "components/ConnectorBlocks";
import { Box } from "components/ui/Box";
import { PageHeader } from "components/ui/PageHeader";
import { StepsIndicator } from "components/ui/StepsIndicator";
import { Text } from "components/ui/Text";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperiment } from "hooks/services/Experiment";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useDestinationList } from "hooks/services/useDestinationHook";
import { useSourceList } from "hooks/services/useSourceHook";
import { InlineEnrollmentCallout } from "packages/cloud/components/experiments/FreeConnectorProgram/InlineEnrollmentCallout";
import { RoutePaths } from "pages/routePaths";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import { ConnectionCreateDestinationForm } from "./ConnectionCreateDestinationForm";
import { ConnectionCreateSourceForm } from "./ConnectionCreateSourceForm";
import ExistingEntityForm from "./ExistingEntityForm";
import { hasDestinationId, hasSourceId, usePreloadData } from "./usePreloadData";

enum StepsTypes {
  CREATE_SOURCE = "source",
  CREATE_DESTINATION = "destination",
  CREATE_CONNECTION = "connection",
}

export const CreateConnectionPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW);
  const location = useLocation();
  const { formatMessage } = useIntl();
  const fcpEnabled = useFeature(FeatureItem.FreeConnectorProgram);
  const workspaceId = useCurrentWorkspaceId();
  const { trackAction } = useAppMonitoringService();
  const isNewFlowActive = useExperiment("connection.updatedConnectionFlow.selectConnectors", false);
  const { sources } = useSourceList();
  const { destinations } = useDestinationList();

  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();

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
      return isNewFlowActive ? (
        <SelectSource onSelectSource={onSelectExistingSource} />
      ) : (
        <>
          {sources.length > 0 && (
            <>
              <ExistingEntityForm type="source" onSubmit={onSelectExistingSource} />
              <Box my="xl">
                <Text align="center" size="lg">
                  <FormattedMessage id="onboarding.or" />
                </Text>
              </Box>
            </>
          )}
          <ConnectionCreateSourceForm
            afterSubmit={() => {
              if (!destination) {
                setCurrentStep(StepsTypes.CREATE_DESTINATION);
              } else {
                setCurrentStep(StepsTypes.CREATE_CONNECTION);
              }
            }}
          />
          <CloudInviteUsersHint connectorType="source" />
        </>
      );
    } else if (currentStep === StepsTypes.CREATE_DESTINATION) {
      return isNewFlowActive ? (
        <SelectDestination onSelectDestination={onSelectExistingDestination} />
      ) : (
        <>
          {destinations.length > 0 && (
            <>
              <ExistingEntityForm type="destination" onSubmit={onSelectExistingDestination} />
              <Box my="xl">
                <Text align="center" size="lg">
                  <FormattedMessage id="onboarding.or" />
                </Text>
              </Box>
            </>
          )}
          <ConnectionCreateDestinationForm
            afterSubmit={() => {
              if (!source) {
                setCurrentStep(StepsTypes.CREATE_SOURCE);
              } else {
                setCurrentStep(StepsTypes.CREATE_CONNECTION);
              }
            }}
          />
          <CloudInviteUsersHint connectorType="destination" />
        </>
      );
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
        pageTitle={
          <PageHeader
            title={<FormattedMessage id="connection.newConnectionTitle" />}
            middleComponent={<StepsIndicator steps={steps} activeStep={currentStep} />}
          />
        }
      >
        {renderStep()}
      </MainPageWithScroll>
    );
  }

  return (
    <>
      <HeadTitle titles={[{ id: "connection.newConnectionTitle" }]} />
      <ConnectorDocumentationWrapper>
        <PageHeader
          title={<FormattedMessage id="connection.newConnectionTitle" />}
          middleComponent={<StepsIndicator steps={steps} activeStep={currentStep} />}
        />
        <FormPageContent>
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
          {fcpEnabled && <InlineEnrollmentCallout withBottomMargin />}
          {renderStep()}
        </FormPageContent>
      </ConnectorDocumentationWrapper>
    </>
  );
};
