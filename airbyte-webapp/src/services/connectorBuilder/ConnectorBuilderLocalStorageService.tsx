import React, { useContext } from "react";

import { BuilderState } from "components/connectorBuilder/types";

import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";

interface LocalStorageContext {
  storedMode: BuilderState["mode"];
  setStoredMode: (view: BuilderState["mode"]) => void;
  checkAssistEnabled: (workspaceId: string) => boolean;
  setAssistEnabledById: (workspaceId: string) => (enabled: boolean) => void;
}

export const ConnectorBuilderLocalStorageContext = React.createContext<LocalStorageContext | null>(null);

export const useAssistEnabled = () => {
  const isAIFeatureEnabled = useExperiment("connectorBuilder.aiAssist.enabled", false);
  const [assistEnabledList, setAssistEnabledList] = useLocalStorage("airbyte_ai-assist-enabled-projects", []);
  const checkAssistEnabled = (workspaceId: string) => assistEnabledList.includes(workspaceId) && isAIFeatureEnabled;

  const setAssistEnabled = (workspaceId: string) => (enabled: boolean) => {
    if (enabled) {
      setAssistEnabledList([...assistEnabledList, workspaceId]);
    } else {
      setAssistEnabledList(assistEnabledList.filter((id) => id !== workspaceId));
    }
  };

  return [checkAssistEnabled, setAssistEnabled] as const;
};

export const ConnectorBuilderLocalStorageProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [storedMode, setStoredMode] = useLocalStorage("connectorBuilderEditorView", "ui");
  const [checkAssistEnabled, setAssistEnabledById] = useAssistEnabled();

  const ctx = {
    storedMode,
    setStoredMode,
    checkAssistEnabled,
    setAssistEnabledById,
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
