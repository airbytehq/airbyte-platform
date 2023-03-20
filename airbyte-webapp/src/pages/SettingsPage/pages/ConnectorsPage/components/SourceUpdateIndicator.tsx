import { useMemo } from "react";

import Indicator from "components/Indicator";

import { useLatestSourceDefinitionList } from "services/connector/SourceDefinitionService";

interface SourceUpdateIndicatorProps {
  id: string;
  currentVersion: string;
}

export const SourceUpdateIndicator: React.FC<SourceUpdateIndicatorProps> = ({ id, currentVersion }) => {
  const { sourceDefinitions } = useLatestSourceDefinitionList();

  const isHidden = useMemo(
    () =>
      sourceDefinitions.find((definition) => definition.sourceDefinitionId === id)?.dockerImageTag === currentVersion,
    [sourceDefinitions, id, currentVersion]
  );

  return <Indicator hidden={isHidden} />;
};
