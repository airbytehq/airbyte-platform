import { SourceDefinitionRead } from "core/api/types/AirbyteClient";

/** From easiest to hardest to set up, as the tech spec ranks the picked sources. */
const EASE_TIERS = ["certifiedManagedOAuth", "certifiedApi", "api", "other", "database"] as const;

type EaseTier = (typeof EASE_TIERS)[number];

const isCertified = (definition: SourceDefinitionRead) => definition.supportLevel === "certified";

/** Only a certified source can use Airbyte-managed OAuth to rank first, so only certified picks need their spec. */
export const getCertifiedDefinitionIds = (definitions: SourceDefinitionRead[]) =>
  definitions.filter(isCertified).map(({ sourceDefinitionId }) => sourceDefinitionId);

const getEaseTier = (definition: SourceDefinitionRead, managedOAuthDefinitionIds: Set<string>): EaseTier => {
  if (isCertified(definition) && managedOAuthDefinitionIds.has(definition.sourceDefinitionId)) {
    return "certifiedManagedOAuth";
  }
  if (definition.sourceType === "api") {
    return isCertified(definition) ? "certifiedApi" : "api";
  }
  return definition.sourceType === "database" ? "database" : "other";
};

/**
 * The easiest source to set up first. The definitions come in pick order, and the sort is stable, so ties keep the
 * pick order.
 */
export const getEasiestSource = (
  definitions: SourceDefinitionRead[],
  managedOAuthDefinitionIds: Set<string>
): SourceDefinitionRead | undefined => {
  const getRank = (definition: SourceDefinitionRead) =>
    EASE_TIERS.indexOf(getEaseTier(definition, managedOAuthDefinitionIds));

  return [...definitions].sort((a, b) => getRank(a) - getRank(b))[0];
};
