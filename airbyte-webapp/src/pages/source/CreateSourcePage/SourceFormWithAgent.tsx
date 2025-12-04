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
import { useGetSourceDefinitionSpecificationAsync } from "core/api";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { Connector } from "core/domain/connector";
import { ForkConnectorButton } from "pages/connectorBuilder/components/ForkConnectorButton";
import { SourcePaths } from "pages/routePaths";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";
import { ConnectorCardValues } from "views/Connector/ConnectorForm/types";

export interface SourceFormValues {
  name: string;
  serviceType: string;
  sourceDefinitionId?: string;
  connectionConfiguration: ConnectionConfiguration;
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
  // save previous path on mount so that it remains unchanged even if search params are added on this page
  const [prevPath] = useState<string>(location.state?.prevPath || `../${SourcePaths.SelectSourceNew}`);
  const onGoBack = () => {
    navigate(prevPath);
  };

  const onSubmitSourceStep = useCallback(
    (sourceValues: { name: string; serviceType: string; connectionConfiguration: Record<string, unknown> }) => {
      return onSubmit({
        ...sourceValues,
        sourceDefinitionId: sourceDefinitionSpecification?.sourceDefinitionId,
      });
    },
    [onSubmit, sourceDefinitionSpecification?.sourceDefinitionId]
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
    setSecrets,
    getSecrets,
    handleClientToolsReady,
    secretInputState,
    handleSecretInputStateChange,
    handleFormValuesReady,
    messages,
    sendMessage,
    isLoading,
    error,
    stopGenerating,
    isStreaming,
  } = useConnectorSetupAgentState({
    actorType: "source",
    actorDefinitionId: selectedSourceDefinitionId,
    connectionSpecification: sourceDefinitionSpecification?.connectionSpecification,
    isAgentView,
  });

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
          onSendMessage={secretInputState.isSecretInputActive ? secretInputState.submitSecret : sendMessage}
          onStop={stopGenerating}
          isSecretMode={secretInputState.isSecretInputActive}
          secretFieldPath={secretInputState.secretFieldPath}
          secretFieldName={secretInputState.secretFieldName}
          isMultiline={secretInputState.isMultiline}
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
                    actorType="source"
                    onSubmitStep={onSubmitSourceStep}
                    setSecrets={setSecrets}
                    getSecrets={getSecrets}
                    onClientToolsReady={handleClientToolsReady}
                    onSecretInputStateChange={handleSecretInputStateChange}
                    onFormValuesReady={handleFormValuesReady}
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
