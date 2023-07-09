import { useMemo } from "react";

import Indicator from "components/Indicator";

import { ReleaseStage } from "core/request/AirbyteClient";
import { useLatestSourceDefinitionList } from "services/connector/SourceDefinitionService";

import { ConnectorCellProps } from "./ConnectorCell";

type SourceUpdateIndicatorProps = Pick<ConnectorCellProps, "id" | "currentVersion" | "releaseStage">;

export const SourceUpdateIndicator: React.FC<SourceUpdateIndicatorProps> = ({ id, currentVersion, releaseStage }) => {
  const { sourceDefinitions } = useLatestSourceDefinitionList();

  const isHidden = useMemo(() => {
    if (releaseStage === ReleaseStage.custom) {
      return true;
    }
    return (
      sourceDefinitions.find((definition) => definition.sourceDefinitionId === id)?.dockerImageTag === currentVersion
    );
  }, [releaseStage, sourceDefinitions, currentVersion, id]);

  return <Indicator hidden={isHidden} />;
};
