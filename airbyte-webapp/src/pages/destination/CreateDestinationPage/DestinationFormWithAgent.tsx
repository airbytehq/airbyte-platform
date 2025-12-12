import React, { useCallback, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";

import { ConnectorSetupAgent } from "components/agents/ConnectorSetupAgent";
import { ConnectorSetupAgentTools } from "components/agents/ConnectorSetupAgentTools";
import { useConnectorSetupAgentState } from "components/agents/hooks/useConnectorSetupAgentState";
import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { FormPageContent } from "components/ConnectorBlocks";
import { MountedViewSwapper } from "components/MountedViewSwapper";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { ConnectionConfiguration } from "area/connector/types";
import { useGetDestinationDefinitionSpecificationAsync } from "core/api";
import { ActorType, DestinationDefinitionRead } from "core/api/types/AirbyteClient";
import { Connector } from "core/domain/connector";
import { DestinationPaths } from "pages/routePaths";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";
import { ConnectorCardValues } from "views/Connector/ConnectorForm/types";

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
