import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation } from "react-router-dom";

import { ConnectorCard } from "area/connector/components/ConnectorCard";
import { ConnectorCardValues } from "area/connector/components/ConnectorForm";
import { ConnectionConfiguration } from "area/connector/types";
import { useGetDestinationDefinitionSpecificationAsync } from "core/api";
import { DestinationDefinitionRead, DestinationRead } from "core/api/types/AirbyteClient";
import { Connector } from "core/domain/connector";

export interface DestinationFormValues {
  name: string;
  serviceType: string;
  destinationDefinitionId?: string;
  connectionConfiguration: ConnectionConfiguration;
}

interface DestinationFormProps {
  onSubmit: (values: DestinationFormValues) => Promise<void>;
  destinationDefinitions: DestinationDefinitionRead[];
  selectedDestinationDefinitionId?: string;
  leftFooterSlot?: React.ReactNode;
  contextLayerOptIn?: React.ReactNode;
  formAutomation?: React.ReactNode;
  onSaveDraft?: (values: DestinationFormValues, existingDraft?: DestinationRead) => Promise<DestinationRead>;
  onDraftPromoted?: (draft: DestinationRead, values: DestinationFormValues) => Promise<void> | void;
}

const hasDestinationDefinitionId = (state: unknown): state is { destinationDefinitionId: string } => {
  return (
    typeof state === "object" &&
    state !== null &&
    typeof (state as { destinationDefinitionId?: string }).destinationDefinitionId === "string"
  );
};

export const DestinationForm: React.FC<DestinationFormProps> = ({
  onSubmit,
  destinationDefinitions,
  selectedDestinationDefinitionId,
  leftFooterSlot = null,
  contextLayerOptIn,
  formAutomation,
  onSaveDraft,
  onDraftPromoted,
}) => {
  const location = useLocation();

  const destinationDefinitionId =
    selectedDestinationDefinitionId ??
    (hasDestinationDefinitionId(location.state) ? location.state.destinationDefinitionId : null);

  const {
    data: destinationDefinitionSpecification,
    error: destinationDefinitionError,
    isLoading,
  } = useGetDestinationDefinitionSpecificationAsync(destinationDefinitionId);

  const selectedDestinationDefinition = useMemo(
    () => destinationDefinitions.find((s) => Connector.id(s) === selectedDestinationDefinitionId),
    [destinationDefinitions, selectedDestinationDefinitionId]
  );

  const onSubmitForm = async (values: ConnectorCardValues) =>
    onSubmit({
      ...values,
      destinationDefinitionId: destinationDefinitionSpecification?.destinationDefinitionId,
    });

  return (
    <ConnectorCard
      formType="destination"
      description={<FormattedMessage id="destinations.description" />}
      isLoading={isLoading}
      fetchingConnectorError={destinationDefinitionError instanceof Error ? destinationDefinitionError : null}
      availableConnectorDefinitions={destinationDefinitions}
      selectedConnectorDefinitionSpecification={destinationDefinitionSpecification}
      selectedConnectorDefinitionId={destinationDefinitionId}
      onSubmit={onSubmitForm}
      onSaveDraft={onSaveDraft}
      onDraftPromoted={onDraftPromoted}
      supportLevel={selectedDestinationDefinition?.supportLevel}
      preFooterSlot={contextLayerOptIn}
      leftFooterSlot={leftFooterSlot}
      formAutomation={formAutomation}
    />
  );
};
