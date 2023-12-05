import { useLatestSourceDefinitionList } from "core/api";

import { VersionCell, VersionCellProps } from "./VersionCell";

type UpdateSourceConnectorVersionCellProps = Omit<VersionCellProps, "latestVersion">;

export const UpdateSourceConnectorVersionCell = (props: UpdateSourceConnectorVersionCellProps) => {
  const { sourceDefinitions } = useLatestSourceDefinitionList();
  const latestVersion = sourceDefinitions.find(
    (sourceDefinitions) => sourceDefinitions.sourceDefinitionId === props.connectorDefinitionId
  )?.dockerImageTag;

  return <VersionCell {...props} latestVersion={latestVersion} />;
};
