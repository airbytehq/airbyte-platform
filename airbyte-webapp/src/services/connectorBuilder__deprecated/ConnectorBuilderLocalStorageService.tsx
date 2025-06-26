import React, { useCallback, useContext } from "react";
import { v4 as uuid } from "uuid";

import { BuilderState } from "components/connectorBuilder__deprecated/types";

import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";

interface LocalStorageContext {
  getStoredMode: (projectId: string) => BuilderState["mode"];
  setStoredMode: (projectId: string, mode: BuilderState["mode"]) => void;
  isAssistProjectEnabled: (projectId: string) => boolean;
  setAssistProjectEnabled: (projectId: string, enabled: boolean, sessionId?: string) => void;
  getAssistProjectSessionId: (projectId: string) => string;
}

export const ConnectorBuilderLocalStorageContext = React.createContext<LocalStorageContext | null>(null);

export const useAssistEnabled = () => {
  const isAIFeatureEnabled = useExperiment("connectorBuilder.aiAssist.enabled");
  const [assistProjects, setAssistProjects] = useLocalStorage("airbyte_ai-assist-projects", {});

  const isAssistProjectEnabled = useCallback(
    (projectId: string) => projectId in assistProjects && isAIFeatureEnabled,
    [assistProjects, isAIFeatureEnabled]
  );
  const setAssistProjectEnabled = useCallback(
    (projectId: string, enabled: boolean, sessionId?: string) => {
      if (enabled) {
        assistProjects[projectId] = { sessionId: sessionId || uuid() };
      } else {
        delete assistProjects[projectId];
      }
      setAssistProjects(assistProjects);
    },
    [assistProjects, setAssistProjects]
  );
  const getAssistProjectSessionId = useCallback(
    (projectId: string) => assistProjects[projectId]?.sessionId || uuid(),
    [assistProjects]
  );

  return [isAssistProjectEnabled, setAssistProjectEnabled, getAssistProjectSessionId] as const;
};

export const ConnectorBuilderLocalStorageProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [storedModes, setStoredModes] = useLocalStorage("airbyte_connector-builder-modes", {});
  const [isAssistProjectEnabled, setAssistProjectEnabled, getAssistProjectSessionId] = useAssistEnabled();

  const getStoredMode = useCallback((projectId: string) => storedModes[projectId] || "ui", [storedModes]);
  const setStoredMode = useCallback(
    (projectId: string, mode: BuilderState["mode"]) => {
      setStoredModes((previousStoredModes) => ({ ...previousStoredModes, [projectId]: mode }));
    },
    [setStoredModes]
  );

  const ctx = {
    getStoredMode,
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
