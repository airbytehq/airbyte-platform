import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";

import { ConnectorSetupAgent } from "components/agents/ConnectorSetupAgent";
import { ConnectorSetupAgentTools } from "components/agents/ConnectorSetupAgentTools";
import { type SecretsMap } from "components/agents/types";
import { type ClientTools, useChatMessages } from "components/chat/hooks/useChatMessages";
import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { FormPageContent } from "components/ConnectorBlocks";
import { MountedViewSwapper } from "components/MountedViewSwapper";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { ConnectionConfiguration } from "area/connector/types";
import { useGetDestinationDefinitionSpecificationAsync } from "core/api";
import { DestinationDefinitionRead } from "core/api/types/AirbyteClient";
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

  // Lift agent state to persist chat messages across toggle
  const [secrets, setSecrets] = useState<SecretsMap>(new Map());
  const secretsRef = useRef<SecretsMap>(secrets);
  useEffect(() => {
    secretsRef.current = secrets;
  }, [secrets]);

  const getSecrets = useCallback(() => secretsRef.current, []);

  // State for client tools (will be set by AgentWithFormContext inside Form)
  const [clientTools, setClientTools] = useState<ClientTools | null>(null);
  const [secretInputState, setSecretInputState] = useState({
    isSecretInputActive: false,
    secretFieldPath: [] as string[],
    secretFieldName: undefined as string | undefined,
    isMultiline: false,
    submitSecret: (() => {}) as (message: string) => void,
  });

  const handleClientToolsReady = useCallback((tools: ClientTools) => {
    setClientTools(tools);
  }, []);

  const handleSecretInputStateChange = useCallback(
    (state: {
      isSecretInputActive: boolean;
      secretFieldPath: string[];
      secretFieldName: string | undefined;
      isMultiline: boolean;
      submitSecret: (message: string) => void;
    }) => {
      setSecretInputState(state);
    },
    []
  );

  const { messages, sendMessage, isLoading, error, stopGenerating, isStreaming } = useChatMessages({
    endpoint: "/agents/connector_setup/chat",
    agentParams: {
      actor_definition_id: selectedDestinationDefinitionId,
      actor_type: "destination",
    },
    clientTools: clientTools || {},
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
                    actorType="destination"
                    onSubmitStep={onSubmitDestinationStep}
                    setSecrets={setSecrets}
                    getSecrets={getSecrets}
                    onClientToolsReady={handleClientToolsReady}
                    onSecretInputStateChange={handleSecretInputStateChange}
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
