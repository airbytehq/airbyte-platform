import React, { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import { Button } from "components/ui/Button";
import { HeadTitle } from "components/ui/HeadTitle";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";
import { ViewToggleButton } from "components/ui/ViewToggleButton";

import { ConnectorSetupViews } from "area/connector/components/agents/ConnectorSetupViews";
import { FormPageContent } from "area/connector/components/ConnectorBlocks";
import { ConnectorDocumentationWrapper } from "area/connector/components/ConnectorDocumentationLayout";
import { DestinationForm } from "area/connector/components/destination/DestinationForm";
import { ConnectionConfiguration, ConnectorSetupFlow } from "area/connector/types";
import { DestinationContextLayerOptIn } from "cloud/components/AgentsOptIn";
import { useShowAgentsOptIn } from "cloud/components/AgentsOptIn/useShowAgentsOptIn";
import {
  useAgentsProvisioningStatus,
  useAgentsSupportedDestinationDefinitionIds,
  useCreateDestination,
  useDestinationDefinitionList,
  useGetDestinationDefinitionSpecificationAsync,
  useSetFusionActorEnablement,
  useUpdateDestination,
} from "core/api";
import { ActorType, DestinationRead } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useExperiment } from "core/services/Experiment";
import { useFormChangeTrackerService } from "core/services/FormChangeTracker";
import { useNotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { clearConnectorChatBuilderStorage, CONNECTOR_CHAT_ACTIONS } from "core/utils/connectorChatBuilderStorage";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { DestinationPaths, RoutePaths } from "pages/routePaths";

import styles from "./CreateDestinationPage.module.scss";

export const CreateDestinationPage: React.FC = () => {
  const { destinationDefinitionId, workspaceId } = useParams<{
    destinationDefinitionId: string;
    workspaceId: string;
  }>();
  useTrackPage(PageTrackingCodes.DESTINATION_NEW);

  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { destinationDefinitions } = useDestinationDefinitionList();
  const { mutateAsync: createDestination } = useCreateDestination();
  const { mutateAsync: updateDestination } = useUpdateDestination();
  const { mutateAsync: setFusionActorEnablement } = useSetFusionActorEnablement();
  const { registerNotification } = useNotificationService();
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });
  const supportedDestinationDefinitionIds = useAgentsSupportedDestinationDefinitionIds();
  const canManage = useGeneratedIntent(Intent.CreateOrEditDestination);
  const [contextLayerAgentAccess, setContextLayerAgentAccess] = useState(canManage);
  useEffect(() => {
    setContextLayerAgentAccess(canManage);
  }, [destinationDefinitionId, canManage]);
  const isAgentAssistedSetupEnabled = useExperiment("connector.agentAssistedSetup");

  const [isAgentView, setIsAgentView] = useState(false);

  const { data: destinationDefinitionSpecification } = useGetDestinationDefinitionSpecificationAsync(
    destinationDefinitionId || null
  );

  // Disable agent for custom connectors since they don't exist in our registry
  // and we don't have access to their specs when the agent is initialized
  const selectedDestinationDefinition = destinationDefinitions.find(
    (d) => d.destinationDefinitionId === destinationDefinitionId
  );
  const isCustomConnector = selectedDestinationDefinition?.custom === true;

  const isAgentAvailable =
    isAgentAssistedSetupEnabled && !isCustomConnector && Boolean(destinationDefinitionSpecification);

  const onSubmitDestinationForm = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration: ConnectionConfiguration;
    setupFlow?: ConnectorSetupFlow;
  }) => {
    const connector = destinationDefinitions.find((item) => item.destinationDefinitionId === values.serviceType);
    if (!connector) {
      throw new Error("No Connector Found");
    }
    const result = await createDestination({
      values,
      destinationConnector: connector,
    });
    const shouldSyncContextLayer =
      isCloudApp &&
      showAgentsOptIn &&
      status?.is_enrolled === true &&
      canManage &&
      values.setupFlow !== "agent" &&
      supportedDestinationDefinitionIds.has(connector.destinationDefinitionId);
    if (shouldSyncContextLayer) {
      void setFusionActorEnablement({
        actorId: result.destinationId,
        actorKind: "destination",
        workspaceId: result.workspaceId,
        enabled: contextLayerAgentAccess,
      }).catch(() => {
        registerNotification({
          id: "cloud.contextLayer.destinationOptIn.syncFailed",
          text: formatMessage({ id: "cloud.contextLayer.destinationOptIn.syncFailed" }),
          type: "error",
        });
      });
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
    clearAllFormChanges();
    navigate(`../${result.destinationId}/${DestinationPaths.Connections}`);
  };

  const onSaveDestinationDraft = async (
    values: {
      name: string;
      serviceType: string;
      connectionConfiguration: ConnectionConfiguration;
      setupFlow?: ConnectorSetupFlow;
    },
    existingDraft?: DestinationRead
  ) => {
    if (existingDraft) {
      return updateDestination({ values, destinationId: existingDraft.destinationId });
    }
    const connector = destinationDefinitions.find((item) => item.destinationDefinitionId === values.serviceType);
    if (!connector) {
      throw new Error("No Connector Found");
    }
    return createDestination({ values: { ...values, createAsDraft: true }, destinationConnector: connector });
  };

  const onDestinationDraftPromoted = async (
    destination: DestinationRead,
    values: { serviceType: string; setupFlow?: ConnectorSetupFlow }
  ) => {
    const connector = destinationDefinitions.find((item) => item.destinationDefinitionId === values.serviceType);
    if (!connector) {
      throw new Error("No Connector Found");
    }
    const shouldSyncContextLayer =
      isCloudApp &&
      showAgentsOptIn &&
      status?.is_enrolled === true &&
      canManage &&
      values.setupFlow !== "agent" &&
      supportedDestinationDefinitionIds.has(connector.destinationDefinitionId);
    if (shouldSyncContextLayer) {
      void setFusionActorEnablement({
        actorId: destination.destinationId,
        actorKind: "destination",
        workspaceId: destination.workspaceId,
        enabled: contextLayerAgentAccess,
      }).catch(() => {
        registerNotification({
          id: "cloud.contextLayer.destinationOptIn.syncFailed",
          text: formatMessage({ id: "cloud.contextLayer.destinationOptIn.syncFailed" }),
          type: "error",
        });
      });
    }
    clearAllFormChanges();
    navigate(`../${destination.destinationId}/${DestinationPaths.Connections}`);
  };

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}`;
  const { formatMessage } = useIntl();

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.destinations" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: formatMessage({ id: "destinations.newDestination" }) },
  ];

  const location = useLocation();
  // save previous path on mount so that it remains unchanged even if search params are added on this page
  const [prevPath] = useState<string>(location.state?.prevPath || `../${DestinationPaths.SelectDestinationNew}`);
  const onGoBack = () => {
    navigate(prevPath);
  };

  const isConnectorBuilderGenerateFromParamsEnabled = useExperiment("connectorBuilder.generateConnectorFromParams");
  useEffect(() => {
    if (isConnectorBuilderGenerateFromParamsEnabled) {
      clearConnectorChatBuilderStorage(CONNECTOR_CHAT_ACTIONS.SET_UP_NEW_CONNECTOR);
    }
  }, [isConnectorBuilderGenerateFromParamsEnabled]);

  const renderSetupForm = (formAutomation?: React.ReactNode) => (
    <ConnectorDocumentationWrapper>
      <FormPageContent>
        <DestinationForm
          onSubmit={onSubmitDestinationForm}
          onSaveDraft={onSaveDestinationDraft}
          onDraftPromoted={onDestinationDraftPromoted}
          destinationDefinitions={destinationDefinitions}
          selectedDestinationDefinitionId={destinationDefinitionId}
          leftFooterSlot={
            <Button variant="clear" onClick={onGoBack} icon="chevronLeft" iconSize="xs">
              <FormattedMessage id="connectorBuilder.backButtonLabel" />
            </Button>
          }
          contextLayerOptIn={
            <DestinationContextLayerOptIn
              destinationDefinitionId={destinationDefinitionId}
              value={contextLayerAgentAccess}
              onChange={setContextLayerAgentAccess}
            />
          }
          formAutomation={formAutomation}
        />
      </FormPageContent>
    </ConnectorDocumentationWrapper>
  );

  const agentSetup = isAgentAssistedSetupEnabled
    ? {
        actorType: ActorType.destination,
        actorDefinitionId: destinationDefinitionId,
        connectionSpecification: destinationDefinitionSpecification?.connectionSpecification,
        isAvailable: isAgentAvailable,
        onSubmit: onSubmitDestinationForm,
        onSaveDraft: onSaveDestinationDraft,
        onDraftPromoted: onDestinationDraftPromoted,
      }
    : undefined;

  return (
    <>
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />
      <div className={styles.pageContainer}>
        <div className={styles.headerWrapper}>
          <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData} />
          {isAgentAvailable && (
            <div className={styles.toggleWrapper}>
              <ViewToggleButton
                leftLabel={formatMessage({ id: "connector.create.toggle.agent", defaultMessage: "Agent" })}
                rightLabel={formatMessage({ id: "connector.create.toggle.form", defaultMessage: "Form" })}
                isRightSelected={!isAgentView}
                onClick={() => setIsAgentView(!isAgentView)}
              />
            </div>
          )}
        </div>
        <div className={styles.contentWrapper}>
          <ConnectorSetupViews agent={agentSetup} isAgentView={isAgentView} renderSetupForm={renderSetupForm} />
        </div>
      </div>
    </>
  );
};
