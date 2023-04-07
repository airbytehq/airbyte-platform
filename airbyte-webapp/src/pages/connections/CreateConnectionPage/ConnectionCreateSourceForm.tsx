import React, { useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { ConnectionConfiguration } from "core/domain/connection";
import { useAvailableSourceDefinitions } from "hooks/domain/connector/useAvailableSourceDefinitions";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateSource } from "hooks/services/useSourceHook";
import { SourceForm } from "pages/source/CreateSourcePage/SourceForm";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

interface ConnectionCreateSourceFormProps {
  afterSubmit: () => void;
}

export const ConnectionCreateSourceForm: React.FC<ConnectionCreateSourceFormProps> = ({ afterSubmit }) => {
  const location = useLocation();
  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const sourceDefinitions = useAvailableSourceDefinitions();
  const { mutateAsync: createSource } = useCreateSource();

  const onSubmitSourceStep = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => {
    const sourceConnector = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!sourceConnector) {
      // Unsure if this can happen, but the types want it defined
      throw new Error("No Connector Found");
    }
    const result = await createSource({ values, sourceConnector });
    await new Promise((resolve) => setTimeout(resolve, 2000));
    clearAllFormChanges();
    navigate(
      {},
      {
        state: {
          ...(location.state as Record<string, unknown>),
          sourceId: result.sourceId,
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

  return <SourceForm onSubmit={onSubmitSourceStep} sourceDefinitions={sourceDefinitions} />;
};
