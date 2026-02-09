import React, { useCallback, useEffect, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { MountedViewSwapper } from "components/ui/MountedViewSwapper";

import { ConnectorSetupAgent } from "area/connector/components/agents/ConnectorSetupAgent";
import { ConnectorSetupAgentTools } from "area/connector/components/agents/ConnectorSetupAgentTools";
import { useConnectorSetupAgentState } from "area/connector/components/agents/hooks/useConnectorSetupAgentState";
import { useConnectorSetupAgentAnalytics } from "area/connector/components/agents/useAnalyticsTrackFunctions";
import { FormPageContent } from "area/connector/components/ConnectorBlocks";
import { ConnectorCard } from "area/connector/components/ConnectorCard";
import { ConnectorDocumentationWrapper } from "area/connector/components/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";
import { ConnectorCardValues } from "area/connector/components/ConnectorForm/types";
import { ConnectionConfiguration } from "area/connector/types";
import { CloudInviteUsersHint } from "area/organization/components/CloudInviteUsersHint";
import { useGetSourceDefinitionSpecificationAsync } from "core/api";
import { ActorType, SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { Connector } from "core/domain/connector";
import { ForkConnectorButton } from "pages/connectorBuilder/components/ForkConnectorButton";
import { SourcePaths } from "pages/routePaths";

export type SourceSetupFlow = "agent" | "form";
export interface SourceFormValues {
  name: string;
  serviceType: string;
  sourceDefinitionId?: string;
  connectionConfiguration: ConnectionConfiguration;
  setupFlow?: SourceSetupFlow;
}

interface SourceFormWithAgentProps {
  isAgentView: boolean;
  onSubmit: (values: SourceFormValues) => Promise<void>;
  sourceDefinitions: SourceDefinitionRead[];
  selectedSourceDefinitionId?: string;
}

export const SourceFormWithAgent: React.FC<SourceFormWithAgentProps> = ({
  isAgentView,
  onSubmit,
  sourceDefinitions,
  selectedSourceDefinitionId,
}) => {
  const { data: sourceDefinitionSpecification } = useGetSourceDefinitionSpecificationAsync(
    selectedSourceDefinitionId || null
  );

  const selectedSourceDefinition = useMemo(
    () => sourceDefinitions.find((s) => Connector.id(s) === selectedSourceDefinitionId),
    [sourceDefinitions, selectedSourceDefinitionId]
  );

  const location = useLocation();
  const navigate = useNavigate();
  const { trackAgentStarted, trackMessageSent, trackConfigurationChecked, trackConfigurationSubmitted } =
    useConnectorSetupAgentAnalytics();
  const [hasTrackedStart, setHasTrackedStart] = useState(false);
  // save previous path on mount so that it remains unchanged even if search params are added on this page
  const [prevPath] = useState<string>(location.state?.prevPath || `../${SourcePaths.SelectSourceNew}`);
  const onGoBack = () => {
    navigate(prevPath);
  };

  const onSubmitSourceStep = useCallback(
    async (sourceValues: { name: string; serviceType: string; connectionConfiguration: Record<string, unknown> }) => {
      try {
        await onSubmit({
          ...sourceValues,
          setupFlow: "agent",
          sourceDefinitionId: sourceDefinitionSpecification?.sourceDefinitionId,
        });
        // Track successful submission
        trackConfigurationSubmitted("source", sourceDefinitionSpecification?.sourceDefinitionId, true);
      } catch (error) {
        // Track failed submission
        trackConfigurationSubmitted("source", sourceDefinitionSpecification?.sourceDefinitionId, false);
        throw error;
      }
    },
    [onSubmit, sourceDefinitionSpecification?.sourceDefinitionId, trackConfigurationSubmitted]
  );

  const onSubmitConnectorCard = useCallback(
    (values: ConnectorCardValues) => {
      return onSubmit({
        ...values,
        sourceDefinitionId: sourceDefinitionSpecification?.sourceDefinitionId,
      });
    },
    [onSubmit, sourceDefinitionSpecification?.sourceDefinitionId]
  );

  // Use the shared hook for all agent/form integration state
  const {
    handleClientToolsReady,
    secretInputState,
    handleSecretInputStateChange,
    oauthState,
    handleOAuthStateChange,
    handleFormValuesReady,
    touchedSecretFieldsRef,
    addTouchedSecretField,
    messages,
    sendMessage,
    isLoading,
    error,
    stopGenerating,
    isStreaming,
  } = useConnectorSetupAgentState({
    actorType: ActorType.source,
    actorDefinitionId: selectedSourceDefinitionId,
    connectionSpecification: sourceDefinitionSpecification?.connectionSpecification,
    isAgentView,
  });

  // Track agent started when first entering agent view
  useEffect(() => {
    if (isAgentView && !hasTrackedStart && selectedSourceDefinitionId) {
      trackAgentStarted("source", selectedSourceDefinitionId);
      setHasTrackedStart(true);
    }
  }, [isAgentView, hasTrackedStart, selectedSourceDefinitionId, trackAgentStarted]);

  // Wrap sendMessage to track analytics (without message content for PII safety)
  const handleSendMessage = useCallback(
    (content: string) => {
      const userMessageCount = messages.filter((msg) => msg.role === "user").length + 1;
      const totalMessageCount = messages.length + 1;
      sendMessage(content);
      trackMessageSent(userMessageCount, totalMessageCount);
    },
    [messages, sendMessage, trackMessageSent]
  );

  // Handle configuration check tracking
  const handleCheckComplete = useCallback(
    (success: boolean) => {
      trackConfigurationChecked("source", selectedSourceDefinitionId, success);
    },
    [trackConfigurationChecked, selectedSourceDefinitionId]
  );

  if (!selectedSourceDefinition || !sourceDefinitionSpecification) {
    return null;
  }

  return (
    <MountedViewSwapper
      isFirstView={isAgentView}
      firstView={
        <ConnectorSetupAgent
          messages={messages}
          isLoading={isLoading}
          error={error}
          isStreaming={isStreaming}
          onSendMessage={secretInputState.isSecretInputActive ? secretInputState.submitSecret : handleSendMessage}
          onStop={stopGenerating}
          isSecretMode={secretInputState.isSecretInputActive}
          secretFieldPath={secretInputState.secretFieldPath}
          secretFieldName={secretInputState.secretFieldName}
          isMultiline={secretInputState.isMultiline}
          isVisible={isAgentView}
          onDismissSecret={secretInputState.dismissSecret}
          oauthState={oauthState}
        />
      }
      secondView={
        <ConnectorDocumentationWrapper>
          <FormPageContent>
            <FlexContainer justifyContent="flex-start">
              <Box mb="md">
                <Button variant="clear" onClick={onGoBack} icon="chevronLeft" iconSize="lg">
                  <FormattedMessage id="connectorBuilder.backButtonLabel" />
                </Button>
              </Box>
            </FlexContainer>
            <ConnectorCard
              formType="source"
              description={<FormattedMessage id="sources.description" />}
              isLoading={false}
              fetchingConnectorError={null}
              availableConnectorDefinitions={sourceDefinitions}
              selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
              selectedConnectorDefinitionId={selectedSourceDefinitionId || null}
              onSubmit={onSubmitConnectorCard}
              supportLevel={selectedSourceDefinition?.supportLevel}
              leftFooterSlot={
                <>
                  {/* Setup tools inside FormProvider so tools can use useFormContext */}
                  <ConnectorSetupAgentTools
                    actorDefinitionId={selectedSourceDefinitionId}
                    actorType={ActorType.source}
                    onSubmitStep={onSubmitSourceStep}
                    onClientToolsReady={handleClientToolsReady}
                    onSecretInputStateChange={handleSecretInputStateChange}
                    onOAuthStateChange={handleOAuthStateChange}
                    onFormValuesReady={handleFormValuesReady}
                    onCheckComplete={handleCheckComplete}
                    touchedSecretFieldsRef={touchedSecretFieldsRef}
                    addTouchedSecretField={addTouchedSecretField}
                  />
                  {selectedSourceDefinition && <ForkConnectorButton sourceDefinition={selectedSourceDefinition} />}
                </>
              }
            />
            <CloudInviteUsersHint connectorType="source" />
          </FormPageContent>
        </ConnectorDocumentationWrapper>
      }
    />
  );
};
