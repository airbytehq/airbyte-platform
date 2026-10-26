import React, { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import { Button } from "components/ui/Button";
import { HeadTitle } from "components/ui/HeadTitle";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";
import { ViewToggleButton } from "components/ui/ViewToggleButton";

import { ConnectorSetupViews } from "area/connector/components/agents/ConnectorSetupViews";
import { FormPageContent } from "area/connector/components/ConnectorBlocks";
import { ConnectorDocumentationWrapper } from "area/connector/components/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";
import { ConnectionConfiguration, ConnectorSetupFlow } from "area/connector/types";
import { SourceContextLayerOptIn } from "cloud/components/AgentsOptIn";
import { useShowAgentsOptIn } from "cloud/components/AgentsOptIn/useShowAgentsOptIn";
import {
  useAgentsProvisioningStatus,
  useAgentsSupportedSourceDefinitionIds,
  useCreateSource,
  useGetSourceDefinitionSpecificationAsync,
  useSetFusionActorEnablement,
  useSourceDefinitionList,
  useUpdateSource,
} from "core/api";
import { ActorType, SourceRead } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useExperiment } from "core/services/Experiment";
import { useFormChangeTrackerService } from "core/services/FormChangeTracker";
import { useNotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { clearConnectorChatBuilderStorage, CONNECTOR_CHAT_ACTIONS } from "core/utils/connectorChatBuilderStorage";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { RoutePaths, SourcePaths } from "pages/routePaths";

import styles from "./CreateSourcePage.module.scss";
import { SourceForm } from "./SourceForm";

export const CreateSourcePage: React.FC = () => {
  const params = useParams<{ workspaceId: string }>();

  const { sourceDefinitionId } = useParams<{ sourceDefinitionId: string }>();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const isAgentAssistedSetupEnabled = useExperiment("connector.agentAssistedSetup");
  const [isAgentView, setIsAgentView] = useState(false);

  const { data: sourceDefinitionSpecification } = useGetSourceDefinitionSpecificationAsync(sourceDefinitionId || null);
  const { sourceDefinitions } = useSourceDefinitionList();
  const { mutateAsync: createSource } = useCreateSource();
  const { mutateAsync: updateSource } = useUpdateSource();
  const { mutateAsync: setFusionActorEnablement } = useSetFusionActorEnablement();
  const { registerNotification } = useNotificationService();
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });
  const supportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds();
  const canManage = useGeneratedIntent(Intent.CreateOrEditSource);
  // Users who cannot change agent access must not be opted in by default.
  const [agentAccessEnabled, setAgentAccessEnabled] = useState(canManage);
  useEffect(() => {
    setAgentAccessEnabled(canManage);
  }, [sourceDefinitionId, canManage]);

  // Disable agent for custom connectors since they don't exist in our registry
  // and we don't have access to their specs when the agent is initialized
  const selectedSourceDefinition = sourceDefinitions.find((s) => s.sourceDefinitionId === sourceDefinitionId);
  const isCustomConnector = selectedSourceDefinition?.custom === true;

  const isAgentAvailable = isAgentAssistedSetupEnabled && !isCustomConnector && Boolean(sourceDefinitionSpecification);

  useTrackPage(PageTrackingCodes.SOURCE_NEW, {
    agent_toggle_available: isAgentAvailable,
  });

  const navigate = useNavigate();
  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Source}`;
  const { formatMessage } = useIntl();

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.sources" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: formatMessage({ id: "sources.newSource" }) },
  ];

  const onSubmitSourceStep = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration: ConnectionConfiguration;
    setupFlow?: ConnectorSetupFlow;
  }) => {
    const connector = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!connector) {
      // Unsure if this can happen, but the types want it defined
      throw new Error("No Connector Found");
    }
    const result = await createSource({ values, sourceConnector: connector });
    const shouldSyncContextLayer =
      isCloudApp &&
      showAgentsOptIn &&
      status?.is_enrolled === true &&
      canManage &&
      values.setupFlow !== "agent" &&
      supportedSourceDefinitionIds.has(connector.sourceDefinitionId);
    if (shouldSyncContextLayer) {
      void setFusionActorEnablement({
        actorId: result.sourceId,
        actorKind: "source",
        workspaceId: result.workspaceId,
        enabled: agentAccessEnabled,
      }).catch(() => {
        registerNotification({
          id: "cloud.contextLayer.sourceOptIn.syncFailed",
          text: formatMessage({ id: "cloud.contextLayer.sourceOptIn.syncFailed" }),
          type: "error",
        });
      });
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
    clearAllFormChanges();
    navigate(`../${result.sourceId}/${SourcePaths.Connections}`);
  };

  const onSaveSourceDraft = async (
    values: {
      name: string;
      serviceType: string;
      connectionConfiguration: ConnectionConfiguration;
      setupFlow?: ConnectorSetupFlow;
    },
    existingDraft?: SourceRead
  ) => {
    if (existingDraft) {
      return updateSource({ values, sourceId: existingDraft.sourceId });
    }
    const connector = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!connector) {
      throw new Error("No Connector Found");
    }
    return createSource({ values: { ...values, createAsDraft: true }, sourceConnector: connector });
  };

  const onSourceDraftPromoted = async (
    source: SourceRead,
    values: { serviceType: string; setupFlow?: ConnectorSetupFlow }
  ) => {
    const connector = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!connector) {
      throw new Error("No Connector Found");
    }
    const shouldSyncContextLayer =
      isCloudApp &&
      showAgentsOptIn &&
      status?.is_enrolled === true &&
      canManage &&
      values.setupFlow !== "agent" &&
      supportedSourceDefinitionIds.has(connector.sourceDefinitionId);
    if (shouldSyncContextLayer) {
      void setFusionActorEnablement({
        actorId: source.sourceId,
        actorKind: "source",
        workspaceId: source.workspaceId,
        enabled: agentAccessEnabled,
      }).catch(() => {
        registerNotification({
          id: "cloud.contextLayer.sourceOptIn.syncFailed",
          text: formatMessage({ id: "cloud.contextLayer.sourceOptIn.syncFailed" }),
          type: "error",
        });
      });
    }
    clearAllFormChanges();
    navigate(`../${source.sourceId}/${SourcePaths.Connections}`);
  };

  const location = useLocation();
  // save previous path on mount so that it remains unchanged even if search params are added on this page
  const [prevPath] = useState<string>(location.state?.prevPath || `../${SourcePaths.SelectSourceNew}`);
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
        <SourceForm
          onSubmit={onSubmitSourceStep}
          onSaveDraft={onSaveSourceDraft}
          onDraftPromoted={onSourceDraftPromoted}
          sourceDefinitions={sourceDefinitions}
          selectedSourceDefinitionId={sourceDefinitionId}
          leftFooterSlot={
            <Button variant="clear" onClick={onGoBack} icon="chevronLeft" iconSize="xs">
              <FormattedMessage id="connectorBuilder.backButtonLabel" />
            </Button>
          }
          contextLayerOptIn={
            <SourceContextLayerOptIn
              sourceDefinitionId={selectedSourceDefinition?.sourceDefinitionId}
              value={agentAccessEnabled}
              onChange={setAgentAccessEnabled}
            />
          }
          formAutomation={formAutomation}
        />
      </FormPageContent>
    </ConnectorDocumentationWrapper>
  );

  const agentSetup = isAgentAssistedSetupEnabled
    ? {
        actorType: ActorType.source,
        actorDefinitionId: sourceDefinitionId,
        connectionSpecification: sourceDefinitionSpecification?.connectionSpecification,
        isAvailable: isAgentAvailable,
        onSubmit: onSubmitSourceStep,
        onSaveDraft: onSaveSourceDraft,
        onDraftPromoted: onSourceDraftPromoted,
      }
    : undefined;

  return (
    <>
      <HeadTitle titles={[{ id: "sources.newSourceTitle" }]} />
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
