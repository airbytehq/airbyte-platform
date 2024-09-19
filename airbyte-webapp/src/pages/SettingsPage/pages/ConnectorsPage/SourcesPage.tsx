import React, { useMemo, useRef } from "react";

import { useListBuilderProjects, useSourceDefinitionList, useSourceList } from "core/api";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import ConnectorsView from "./components/ConnectorsView";

const SourcesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_SOURCE);
  const connectorBuilderProjects = useListBuilderProjects();
  const { sources } = useSourceList();
  const { sourceDefinitions } = useSourceDefinitionList();

  const idToSourceDefinition = useMemo(
    () =>
      sourceDefinitions.reduce((map, sourceDefinition) => {
        map.set(sourceDefinition.sourceDefinitionId, sourceDefinition);
        return map;
      }, new Map<string, SourceDefinitionRead>()),
    [sourceDefinitions]
  );
  const definitionMap = useRef(idToSourceDefinition);
  definitionMap.current = idToSourceDefinition;

  const usedSourceDefinitions: SourceDefinitionRead[] = useMemo(() => {
    const usedSourceDefinitionIds = new Set<string>(sources.map((source) => source.sourceDefinitionId));
    return sourceDefinitions
      .filter((sourceDefinition) => usedSourceDefinitionIds.has(sourceDefinition.sourceDefinitionId))
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [sourceDefinitions, sources]);

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
