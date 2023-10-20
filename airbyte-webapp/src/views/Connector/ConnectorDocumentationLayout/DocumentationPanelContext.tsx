import { createContext, useContext, useState } from "react";

import { ConnectorDefinition } from "core/domain/connector";

export type DocumentationPanelContext = ReturnType<typeof useDocumentationPanelState>;

export const useDocumentationPanelState = () => {
  const [documentationPanelOpen, setDocumentationPanelOpen] = useState(false);
  const [selectedConnectorDefinition, setSelectedConnectorDefinition] = useState<ConnectorDefinition>();
  const [focusedField, setFocusedField] = useState<string | undefined>();

  return {
    documentationPanelOpen,
    setDocumentationPanelOpen,
    selectedConnectorDefinition,
    setSelectedConnectorDefinition,
    focusedField,
    setFocusedField,
  };
};

// @ts-expect-error Default value provided at implementation
export const documentationPanelContext = createContext<DocumentationPanelContext>();

export const useDocumentationPanelContext = () => useContext(documentationPanelContext);
export const useOptionalDocumentationPanelContext = () =>
  useContext(documentationPanelContext) as DocumentationPanelContext | undefined;

export const DocumentationPanelProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  return (
    <documentationPanelContext.Provider value={useDocumentationPanelState()}>
      {children}
    </documentationPanelContext.Provider>
  );
};
