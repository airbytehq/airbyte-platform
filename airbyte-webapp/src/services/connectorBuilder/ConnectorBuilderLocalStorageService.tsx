import React, { useContext } from "react";

import { BuilderState } from "components/connectorBuilder/types";

import { useLocalStorage } from "core/utils/useLocalStorage";

interface LocalStorageContext {
  storedMode: BuilderState["mode"];
  setStoredMode: (view: BuilderState["mode"]) => void;
}

export const ConnectorBuilderLocalStorageContext = React.createContext<LocalStorageContext | null>(null);

export const ConnectorBuilderLocalStorageProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [storedMode, setStoredMode] = useLocalStorage("connectorBuilderEditorView", "ui");

  const ctx = {
    storedMode,
    setStoredMode,
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
