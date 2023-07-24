import React, { useContext } from "react";

import { EditorView } from "components/connectorBuilder/types";

import { useLocalStorageFixed } from "core/utils/useLocalStorageFixed";

interface LocalStorageContext {
  storedEditorView: EditorView;
  setStoredEditorView: (view: EditorView) => void;
}

export const ConnectorBuilderLocalStorageContext = React.createContext<LocalStorageContext | null>(null);

export const ConnectorBuilderLocalStorageProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [storedEditorView, setStoredEditorView] = useLocalStorageFixed<EditorView>("connectorBuilderEditorView", "ui");

  const ctx = {
    storedEditorView,
    setStoredEditorView,
  };

  return (
    <ConnectorBuilderLocalStorageContext.Provider value={ctx}>{children}</ConnectorBuilderLocalStorageContext.Provider>
  );
};

export const useConnectorBuilderLocalStorage = (): LocalStorageContext => {
  const connectorBuilderLocalStorage = useContext(ConnectorBuilderLocalStorageContext);
  if (!connectorBuilderLocalStorage) {
    throw new Error("useConnectorBuilderLocalStorage must be used within a ConnectorBuilderLocalStorageProvider.");
  }

  return connectorBuilderLocalStorage;
};
