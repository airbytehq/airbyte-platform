import { useLatestDestinationDefinitionList } from "services/connector/DestinationDefinitionService";

import VersionCell from "./VersionCell";
import { VersionCellProps } from "./VersionCell";

type UpdateDestinationConnectorVersionCellProps = Omit<VersionCellProps, "latestVersion">;

export const UpdateDestinationConnectorVersionCell = (props: UpdateDestinationConnectorVersionCellProps) => {
  const { destinationDefinitions } = useLatestDestinationDefinitionList();
  const latestVersion = destinationDefinitions.find(
    (destinationDefinition) => destinationDefinition.destinationDefinitionId === props.id
  )?.dockerImageTag;

  return <VersionCell {...props} latestVersion={latestVersion} />;
};
