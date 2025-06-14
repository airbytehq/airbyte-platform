import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation } from "react-router-dom";

import { ConnectorDefinitionBranding } from "components/ui/ConnectorDefinitionBranding";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { ConnectionConfiguration } from "area/connector/types";
import { useGetSourceDefinitionSpecificationAsync } from "core/api";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { Connector } from "core/domain/connector";
import { ForkConnectorButton } from "pages/connectorBuilder/components/ForkConnectorButton";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorCardValues } from "views/Connector/ConnectorForm/types";

export interface SourceFormValues {
  name: string;
  serviceType: string;
  sourceDefinitionId?: string;
  connectionConfiguration: ConnectionConfiguration;
}

interface SourceFormProps {
  onSubmit: (values: SourceFormValues) => Promise<void>;
  sourceDefinitions: SourceDefinitionRead[];
  selectedSourceDefinitionId?: string;
}

const hasSourceDefinitionId = (state: unknown): state is { sourceDefinitionId: string } => {
  return (
    typeof state === "object" &&
    state !== null &&
    typeof (state as { sourceDefinitionId?: string }).sourceDefinitionId === "string"
  );
};

export const SourceForm: React.FC<SourceFormProps> = ({ onSubmit, sourceDefinitions, selectedSourceDefinitionId }) => {
  const location = useLocation();

  const sourceDefinitionId =
    selectedSourceDefinitionId ?? (hasSourceDefinitionId(location.state) ? location.state.sourceDefinitionId : null);

  const {
    data: sourceDefinitionSpecification,
    error: sourceDefinitionError,
    isLoading,
  } = useGetSourceDefinitionSpecificationAsync(sourceDefinitionId);

  const selectedSourceDefinition = useMemo(
    () => sourceDefinitions.find((s) => Connector.id(s) === selectedSourceDefinitionId),
    [sourceDefinitions, selectedSourceDefinitionId]
  );

  const onSubmitForm = (values: ConnectorCardValues) =>
    onSubmit({
      ...values,
      sourceDefinitionId: sourceDefinitionSpecification?.sourceDefinitionId,
    });

  const HeaderBlock = () => {
    return (
      <FlexContainer justifyContent="space-between">
        <Heading as="h3" size="sm">
          <FormattedMessage id="onboarding.createSource" />
        </Heading>
        {selectedSourceDefinitionId && <ConnectorDefinitionBranding sourceDefinitionId={selectedSourceDefinitionId} />}
      </FlexContainer>
    );
  };

  return (
    <ConnectorCard
      formType="source"
      description={<FormattedMessage id="sources.description" />}
      headerBlock={<HeaderBlock />}
      isLoading={isLoading}
      fetchingConnectorError={sourceDefinitionError instanceof Error ? sourceDefinitionError : null}
      availableConnectorDefinitions={sourceDefinitions}
      selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
      selectedConnectorDefinitionId={sourceDefinitionId}
      onSubmit={onSubmitForm}
      supportLevel={selectedSourceDefinition?.supportLevel}
      leftFooterSlot={selectedSourceDefinition && <ForkConnectorButton sourceDefinition={selectedSourceDefinition} />}
    />
  );
};
