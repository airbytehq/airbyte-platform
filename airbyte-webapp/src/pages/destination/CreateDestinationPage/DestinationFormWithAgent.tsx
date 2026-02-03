import React, { useCallback, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { MountedViewSwapper } from "components/ui/MountedViewSwapper";

import { ConnectorSetupAgent } from "area/connector/components/agents/ConnectorSetupAgent";
import { ConnectorSetupAgentTools } from "area/connector/components/agents/ConnectorSetupAgentTools";
import { useConnectorSetupAgentState } from "area/connector/components/agents/hooks/useConnectorSetupAgentState";
import { FormPageContent } from "area/connector/components/ConnectorBlocks";
import { ConnectorCard } from "area/connector/components/ConnectorCard";
import { ConnectorDocumentationWrapper } from "area/connector/components/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";
import { ConnectorCardValues } from "area/connector/components/ConnectorForm/types";
import { ConnectionConfiguration } from "area/connector/types";
import { CloudInviteUsersHint } from "area/organization/components/CloudInviteUsersHint";
import { useGetDestinationDefinitionSpecificationAsync } from "core/api";
import { ActorType, DestinationDefinitionRead } from "core/api/types/AirbyteClient";
import { Connector } from "core/domain/connector";
import { DestinationPaths } from "pages/routePaths";

export interface DestinationFormValues {
  name: string;
  serviceType: string;
  destinationDefinitionId?: string;
  connectionConfiguration: ConnectionConfiguration;
}

interface DestinationFormWithAgentProps {
  isAgentView: boolean;
  onSubmit: (values: DestinationFormValues) => Promise<void>;
  destinationDefinitions: DestinationDefinitionRead[];
  selectedDestinationDefinitionId?: string;
}

export const DestinationFormWithAgent: React.FC<DestinationFormWithAgentProps> = ({
  isAgentView,
  onSubmit,
  destinationDefinitions,
  selectedDestinationDefinitionId,
}) => {
  const { data: destinationDefinitionSpecification } = useGetDestinationDefinitionSpecificationAsync(
    selectedDestinationDefinitionId || null
  );

  const selectedDestinationDefinition = useMemo(
    () => destinationDefinitions.find((d) => Connector.id(d) === selectedDestinationDefinitionId),
    [destinationDefinitions, selectedDestinationDefinitionId]
  );

  const location = useLocation();
  const navigate = useNavigate();
  // save previous path on mount so that it remains unchanged even if search params are added on this page
  const [prevPath] = useState<string>(location.state?.prevPath || `../${DestinationPaths.SelectDestinationNew}`);
  const onGoBack = () => {
    navigate(prevPath);
  };

  const onSubmitDestinationStep = useCallback(
    (destinationValues: { name: string; serviceType: string; connectionConfiguration: Record<string, unknown> }) => {
      return onSubmit({
        ...destinationValues,
        destinationDefinitionId: destinationDefinitionSpecification?.destinationDefinitionId,
      });
    },
    [onSubmit, destinationDefinitionSpecification?.destinationDefinitionId]
  );

  const onSubmitConnectorCard = useCallback(
    (values: ConnectorCardValues) => {
      return onSubmit({
        ...values,
        destinationDefinitionId: destinationDefinitionSpecification?.destinationDefinitionId,
      });
    },
    [onSubmit, destinationDefinitionSpecification?.destinationDefinitionId]
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
    actorType: ActorType.destination,
    actorDefinitionId: selectedDestinationDefinitionId,
    connectionSpecification: destinationDefinitionSpecification?.connectionSpecification,
    isAgentView,
  });

  if (!selectedDestinationDefinition || !destinationDefinitionSpecification) {
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
          onSendMessage={secretInputState.isSecretInputActive ? secretInputState.submitSecret : sendMessage}
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
              formType="destination"
              description={<FormattedMessage id="destinations.description" />}
              isLoading={false}
              fetchingConnectorError={null}
              availableConnectorDefinitions={destinationDefinitions}
              selectedConnectorDefinitionSpecification={destinationDefinitionSpecification}
              selectedConnectorDefinitionId={selectedDestinationDefinitionId || null}
              onSubmit={onSubmitConnectorCard}
              supportLevel={selectedDestinationDefinition?.supportLevel}
              leftFooterSlot={
                <>
                  {/* Setup tools inside FormProvider so saveDraftTool can use useFormContext */}
                  <ConnectorSetupAgentTools
                    actorDefinitionId={selectedDestinationDefinitionId}
                    actorType={ActorType.destination}
                    onSubmitStep={onSubmitDestinationStep}
                    onClientToolsReady={handleClientToolsReady}
                    onSecretInputStateChange={handleSecretInputStateChange}
                    onOAuthStateChange={handleOAuthStateChange}
                    onFormValuesReady={handleFormValuesReady}
                    touchedSecretFieldsRef={touchedSecretFieldsRef}
                    addTouchedSecretField={addTouchedSecretField}
                  />
                </>
              }
            />
            <CloudInviteUsersHint connectorType="destination" />
          </FormPageContent>
        </ConnectorDocumentationWrapper>
      }
    />
  );
};
