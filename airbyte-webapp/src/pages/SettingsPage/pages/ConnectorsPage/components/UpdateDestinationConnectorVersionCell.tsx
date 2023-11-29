import { useLatestDestinationDefinitionList } from "core/api";

import { VersionCell, VersionCellProps } from "./VersionCell";

type UpdateDestinationConnectorVersionCellProps = Omit<VersionCellProps, "latestVersion">;

export const UpdateDestinationConnectorVersionCell = (props: UpdateDestinationConnectorVersionCellProps) => {
  const { destinationDefinitions } = useLatestDestinationDefinitionList();
  const latestVersion = destinationDefinitions.find(
    (destinationDefinition) => destinationDefinition.destinationDefinitionId === props.connectorDefinitionId
  )?.dockerImageTag;

  return <VersionCell {...props} latestVersion={latestVersion} />;
};
