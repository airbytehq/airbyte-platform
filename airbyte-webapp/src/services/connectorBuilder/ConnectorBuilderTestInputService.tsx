import React, { useContext, useState } from "react";
import { useParams } from "react-router-dom";

import { ConnectorConfig } from "core/api/types/ConnectorBuilderClient";

interface ProjectToTestInputState {
  projectToTestInputJson: Record<string, TestInputContext["testInputJson"]>;
  setProjectToTestInputJson: (projectId: string, value: TestInputContext["testInputJson"]) => void;
}

interface TestInputContext {
  testInputJson: ConnectorConfig | undefined;
  setTestInputJson: (value: TestInputContext["testInputJson"]) => void;
}

export const ConnectorBuilderTestInputContext = React.createContext<ProjectToTestInputState | null>(null);

export const ConnectorBuilderTestInputProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  // config
  const [projectToTestInputJson, setTestInputJson] = useState<Record<string, TestInputContext["testInputJson"]>>({});

  const ctx = {
    projectToTestInputJson,
    setProjectToTestInputJson: (projectId: string, value: TestInputContext["testInputJson"]) => {
      setTestInputJson((prev) => {
        return { ...prev, [projectId]: value };
      });
    },
  };

  return <ConnectorBuilderTestInputContext.Provider value={ctx}>{children}</ConnectorBuilderTestInputContext.Provider>;
};

export const useConnectorBuilderTestInputState = (): TestInputContext => {
  const { projectId } = useParams<{
    projectId: string;
  }>();
  if (!projectId) {
    throw new Error("Could not find project id in path");
  }
  const connectorBuilderState = useContext(ConnectorBuilderTestInputContext);
  if (!connectorBuilderState) {
    throw new Error("useConnectorBuilderTestInputState must be used within a ConnectorBuilderTestStateProvider.");
  }

  return {
    testInputJson: connectorBuilderState.projectToTestInputJson[projectId],
    setTestInputJson: (value: ConnectorConfig | undefined) =>
      connectorBuilderState.setProjectToTestInputJson(projectId, value),
  };
};
