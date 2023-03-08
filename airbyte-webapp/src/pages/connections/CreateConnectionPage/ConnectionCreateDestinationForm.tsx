import React, { useEffect } from "react";
// TODO: create separate component for source and destinations forms
import { useLocation, useNavigate } from "react-router-dom";

import { DestinationForm } from "components/destination/DestinationForm";

import { ConnectionConfiguration } from "core/domain/connection";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateDestination } from "hooks/services/useDestinationHook";
import { useDestinationDefinitionList } from "services/connector/DestinationDefinitionService";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

interface ConnectionCreateDestinationFormProps {
  afterSubmit: () => void;
}

export const ConnectionCreateDestinationForm: React.FC<ConnectionCreateDestinationFormProps> = ({ afterSubmit }) => {
  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const location = useLocation();

  const { destinationDefinitions } = useDestinationDefinitionList();
  const { mutateAsync: createDestination } = useCreateDestination();

  const onSubmitDestinationForm = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => {
    const connector = destinationDefinitions.find((item) => item.destinationDefinitionId === values.serviceType);
    const result = await createDestination({
      values,
      destinationConnector: connector,
    });
    await new Promise((resolve) => setTimeout(resolve, 2000));
    clearAllFormChanges();
    navigate(
      {},
      {
        state: {
          ...(location.state as Record<string, unknown>),
          destinationId: result.destinationId,
        },
      }
    );
    afterSubmit();
  };

  const { setDocumentationPanelOpen } = useDocumentationPanelContext();

  useEffect(() => {
    return () => {
      setDocumentationPanelOpen(false);
    };
  }, [setDocumentationPanelOpen]);

  return <DestinationForm onSubmit={onSubmitDestinationForm} destinationDefinitions={destinationDefinitions} />;
};
