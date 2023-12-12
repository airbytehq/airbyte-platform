import dayjs from "dayjs";

import {
  ActorDefinitionVersionRead,
  DestinationDefinitionSpecificationRead,
  SourceDefinitionSpecificationRead,
} from "core/api/types/AirbyteClient";

import { isSource, isSourceDefinition, isSourceDefinitionSpecification } from "./source";
import { ConnectorDefinition, ConnectorT } from "./types";

export class Connector {
  static id(connector: ConnectorDefinition): string {
    return isSourceDefinition(connector) ? connector.sourceDefinitionId : connector.destinationDefinitionId;
  }
}

export class ConnectorHelper {
  static id(connector: ConnectorT): string {
    return isSource(connector) ? connector.sourceId : connector.destinationId;
  }
}

export class ConnectorSpecification {
  static id(connector: DestinationDefinitionSpecificationRead | SourceDefinitionSpecificationRead): string {
    return isSourceDefinitionSpecification(connector)
      ? connector.sourceDefinitionId
      : connector.destinationDefinitionId;
  }
}

export const shouldDisplayBreakingChangeBanner = (actorDefinitionVersion: ActorDefinitionVersionRead): boolean => {
  const hasUpcomingBreakingChanges =
    !!actorDefinitionVersion?.breakingChanges &&
    actorDefinitionVersion.breakingChanges.upcomingBreakingChanges.length > 0;

  // This is important as it catches the case where a user has been explicitly pinned to a previous version
  // e.g. Prereleases, PbA Users etc..
  const actorNotOverriden = !actorDefinitionVersion.isOverrideApplied;

  return hasUpcomingBreakingChanges && actorNotOverriden;
};

/**
 * Format the upgrade deadline for a given actor definition version to a human readable format
 * @param actorDefinitionVersion The actor definition version to format the upgrade deadline for
 * @returns The formatted upgrade deadline or null if there is no deadline
 */
export const getHumanReadableUpgradeDeadline = (actorDefinitionVersion: ActorDefinitionVersionRead): string | null => {
  const deadline = actorDefinitionVersion.breakingChanges?.minUpgradeDeadline;
  if (deadline) {
    return dayjs(deadline).format("MMMM D, YYYY");
  }
  return null;
};
