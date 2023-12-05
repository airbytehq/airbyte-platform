import { useMemo } from "react";

import Indicator from "components/Indicator";

import { useLatestSourceDefinitionList } from "core/api";

import { ConnectorCellProps } from "./ConnectorCell";

type SourceUpdateIndicatorProps = Pick<ConnectorCellProps, "id" | "currentVersion"> & { custom: boolean };

export const SourceUpdateIndicator: React.FC<SourceUpdateIndicatorProps> = ({ id, currentVersion, custom }) => {
  const { sourceDefinitions } = useLatestSourceDefinitionList();

  const isHidden = useMemo(() => {
    if (custom) {
      return true;
    }
    return (
      sourceDefinitions.find((definition) => definition.sourceDefinitionId === id)?.dockerImageTag === currentVersion
    );
  }, [custom, sourceDefinitions, currentVersion, id]);

  return <Indicator hidden={isHidden} />;
};
