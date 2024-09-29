import React, { useContext } from "react";
import { v4 as uuid } from "uuid";

import { BuilderState } from "components/connectorBuilder/types";

import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";

interface LocalStorageContext {
  storedMode: BuilderState["mode"];
  setStoredMode: (view: BuilderState["mode"]) => void;
  isAssistProjectEnabled: (projectId: string) => boolean;
  setAssistProjectEnabled: (projectId: string, enabled: boolean, sessionId?: string) => void;
  getAssistProjectSessionId: (projectId: string) => string;
}

export const ConnectorBuilderLocalStorageContext = React.createContext<LocalStorageContext | null>(null);

export const useAssistEnabled = () => {
  const isAIFeatureEnabled = useExperiment("connectorBuilder.aiAssist.enabled");
  const [assistProjects, setAssistProjects] = useLocalStorage("airbyte_ai-assist-projects", {});

  const isAssistProjectEnabled = (projectId: string) => projectId in assistProjects && isAIFeatureEnabled;
  const setAssistProjectEnabled = (projectId: string, enabled: boolean, sessionId?: string) => {
    if (enabled) {
      assistProjects[projectId] = { sessionId: sessionId || uuid() };
    } else {
      delete assistProjects[projectId];
    }
    setAssistProjects(assistProjects);
  };
  const getAssistProjectSessionId = (projectId: string) => assistProjects[projectId]?.sessionId || uuid();

  return [isAssistProjectEnabled, setAssistProjectEnabled, getAssistProjectSessionId] as const;
};

export const ConnectorBuilderLocalStorageProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [storedMode, setStoredMode] = useLocalStorage("connectorBuilderEditorView", "ui");
  const [isAssistProjectEnabled, setAssistProjectEnabled, getAssistProjectSessionId] = useAssistEnabled();

  const ctx = {
    storedMode,
    setStoredMode,
    isAssistProjectEnabled,
    setAssistProjectEnabled,
    getAssistProjectSessionId,
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
