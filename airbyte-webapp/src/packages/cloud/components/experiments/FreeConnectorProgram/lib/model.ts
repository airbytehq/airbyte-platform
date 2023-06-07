import { DestinationDefinitionRead, SourceDefinitionRead, WebBackendConnectionRead } from "core/request/AirbyteClient";

export const freeReleaseStages = ["alpha", "beta"];

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
