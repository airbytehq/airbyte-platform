import {
  DestinationDefinitionRead,
  ReleaseStage,
  SourceDefinitionRead,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";

export const freeReleaseStages: ReleaseStage[] = ["alpha", "beta"];

export const isConnectionEligibleForFCP = (
  connection: WebBackendConnectionRead,
  sourceDefinition: SourceDefinitionRead,
  destinationDefinition: DestinationDefinitionRead
): boolean => {
  return !!(
    connection.status !== "deprecated" &&
    sourceDefinition.releaseStage &&
    destinationDefinition.releaseStage &&
    (freeReleaseStages.includes(sourceDefinition.releaseStage) ||
      freeReleaseStages.includes(destinationDefinition.releaseStage))
  );
};

export const isSourceDefinitionEligibleForFCP = (sourceDefinition: SourceDefinitionRead): boolean => {
  return !!(sourceDefinition.releaseStage && freeReleaseStages.includes(sourceDefinition.releaseStage));
};

export const isDestinationDefinitionEligibleForFCP = (destinationDefinition: DestinationDefinitionRead): boolean => {
  return !!(destinationDefinition.releaseStage && freeReleaseStages.includes(destinationDefinition.releaseStage));
};
