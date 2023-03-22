import { useLatestSourceDefinitionList } from "services/connector/SourceDefinitionService";

import VersionCell from "./VersionCell";
import { VersionCellProps } from "./VersionCell";

type UpdateSourceConnectorVersionCellProps = Omit<VersionCellProps, "latestVersion">;

export const UpdateSourceConnectorVersionCell = (props: UpdateSourceConnectorVersionCellProps) => {
  const { sourceDefinitions } = useLatestSourceDefinitionList();
  const latestVersion = sourceDefinitions.find(
    (sourceDefinitions) => sourceDefinitions.sourceDefinitionId === props.id
  )?.dockerImageTag;

  return <VersionCell {...props} latestVersion={latestVersion} />;
};
