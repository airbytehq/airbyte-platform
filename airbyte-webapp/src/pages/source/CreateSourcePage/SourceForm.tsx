import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation } from "react-router-dom";

import { ConnectorCard } from "area/connector/components/ConnectorCard";
import { ConnectorCardValues } from "area/connector/components/ConnectorForm/types";
import { ConnectionConfiguration } from "area/connector/types";
import { useGetSourceDefinitionSpecificationAsync } from "core/api";
import { SourceDefinitionRead, SourceRead } from "core/api/types/AirbyteClient";
import { Connector } from "core/domain/connector";

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
  leftFooterSlot?: React.ReactNode;
  contextLayerOptIn?: React.ReactNode;
  formAutomation?: React.ReactNode;
  onSaveDraft?: (values: SourceFormValues, existingDraft?: SourceRead) => Promise<SourceRead>;
  onDraftPromoted?: (draft: SourceRead, values: SourceFormValues) => Promise<void> | void;
}

const hasSourceDefinitionId = (state: unknown): state is { sourceDefinitionId: string } => {
  return (
    typeof state === "object" &&
    state !== null &&
    typeof (state as { sourceDefinitionId?: string }).sourceDefinitionId === "string"
  );
};

export const SourceForm: React.FC<SourceFormProps> = ({
  onSubmit,
  sourceDefinitions,
  selectedSourceDefinitionId,
  leftFooterSlot = null,
  contextLayerOptIn,
  formAutomation,
  onSaveDraft,
  onDraftPromoted,
}) => {
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

  return (
    <ConnectorCard
      formType="source"
      description={<FormattedMessage id="sources.description" />}
      isLoading={isLoading}
      fetchingConnectorError={sourceDefinitionError instanceof Error ? sourceDefinitionError : null}
      availableConnectorDefinitions={sourceDefinitions}
      selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
      selectedConnectorDefinitionId={sourceDefinitionId}
      onSubmit={onSubmitForm}
      onSaveDraft={onSaveDraft}
      onDraftPromoted={onDraftPromoted}
      supportLevel={selectedSourceDefinition?.supportLevel}
      preFooterSlot={contextLayerOptIn}
      leftFooterSlot={leftFooterSlot}
      formAutomation={formAutomation}
    />
  );
};
