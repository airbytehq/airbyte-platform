import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation } from "react-router-dom";

import { ConnectionConfiguration } from "core/domain/connection";
import { SourceDefinitionRead } from "core/request/AirbyteClient";
import { LogsRequestError } from "core/request/LogsRequestError";
import { useGetSourceDefinitionSpecificationAsync } from "services/connector/SourceDefinitionSpecificationService";
import { FormError } from "utils/errorStatusMessage";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorCardValues } from "views/Connector/ConnectorForm/types";

export interface SourceFormValues {
  name: string;
  serviceType: string;
  sourceDefinitionId?: string;
  connectionConfiguration?: ConnectionConfiguration;
}

interface SourceFormProps {
  onSubmit: (values: SourceFormValues) => Promise<void>;
  sourceDefinitions: SourceDefinitionRead[];
  error?: FormError | null;
  selectedSourceDefinitionId?: string;
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
  error,
  selectedSourceDefinitionId,
}) => {
  const location = useLocation();

  const [sourceDefinitionId, setSourceDefinitionId] = useState<string | null>(
    selectedSourceDefinitionId ?? (hasSourceDefinitionId(location.state) ? location.state.sourceDefinitionId : null)
  );

  const {
    data: sourceDefinitionSpecification,
    error: sourceDefinitionError,
    isLoading,
  } = useGetSourceDefinitionSpecificationAsync(sourceDefinitionId);

  const onDropDownSelect = (sourceDefinitionId: string) => {
    setSourceDefinitionId(sourceDefinitionId);
  };

  const onSubmitForm = (values: ConnectorCardValues) =>
    onSubmit({
      ...values,
      sourceDefinitionId: sourceDefinitionSpecification?.sourceDefinitionId,
    });

  return (
    <ConnectorCard
      formType="source"
      title={<FormattedMessage id="onboarding.sourceSetUp" />}
      description={<FormattedMessage id="sources.description" />}
      isLoading={isLoading}
      fetchingConnectorError={sourceDefinitionError instanceof Error ? sourceDefinitionError : null}
      availableConnectorDefinitions={sourceDefinitions}
      onConnectorDefinitionSelect={onDropDownSelect}
      selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
      selectedConnectorDefinitionId={sourceDefinitionId}
      onSubmit={onSubmitForm}
      jobInfo={LogsRequestError.extractJobInfo(error)}
    />
  );
};
