import React from "react";

import { useListBuilderProjects, useSourceDefinitionList } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import ConnectorsView from "./components/ConnectorsView";

const SourcesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_SOURCE);
  const connectorBuilderProjects = useListBuilderProjects();
  const { sourceDefinitions } = useSourceDefinitionList();
  const { sourceDefinitions: usedSourceDefinitions } = useSourceDefinitionList({ filterByUsed: true });

  return (
    <ConnectorsView
      type="sources"
      usedConnectorsDefinitions={usedSourceDefinitions}
      connectorsDefinitions={sourceDefinitions}
      connectorBuilderProjects={connectorBuilderProjects}
    />
  );
};

export default SourcesPage;
