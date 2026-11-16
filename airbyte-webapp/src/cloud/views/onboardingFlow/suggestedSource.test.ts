import { SourceDefinitionRead, SupportLevel } from "core/api/types/AirbyteClient";

import { getEasiestSource } from "./suggestedSource";

const definition = (
  sourceDefinitionId: string,
  sourceType: SourceDefinitionRead["sourceType"],
  supportLevel: SupportLevel
): SourceDefinitionRead => ({
  sourceDefinitionId,
  name: sourceDefinitionId,
  dockerRepository: `airbyte/${sourceDefinitionId}`,
  dockerImageTag: "1.0.0",
  sourceType,
  supportLevel,
});

const postgres = definition("postgres", "database", "certified");
const file = definition("file", "file", "certified");
const communityApi = definition("communityApi", "api", "community");
const certifiedApi = definition("certifiedApi", "api", "certified");
const shopify = definition("shopify", "api", "certified");
const klaviyo = definition("klaviyo", "api", "certified");
const communityOAuth = definition("communityOAuth", "api", "community");

const MANAGED_OAUTH_DEFINITION_IDS = new Set([shopify.sourceDefinitionId, communityOAuth.sourceDefinitionId]);

describe("getEasiestSource", () => {
  it.each([
    ["a certified source with managed OAuth before a certified API source", [certifiedApi, shopify], shopify],
    ["a certified API source before a community API source", [communityApi, certifiedApi], certifiedApi],
    ["an API source before a file source", [file, communityApi], communityApi],
    ["a file source before a database source", [postgres, file], file],
    [
      "a certified API source before a community source with managed OAuth",
      [communityOAuth, certifiedApi],
      certifiedApi,
    ],
    ["the first pick when two sources tie", [klaviyo, certifiedApi], klaviyo],
  ])("picks %s", (_, picks, expected) => {
    expect(getEasiestSource(picks, MANAGED_OAUTH_DEFINITION_IDS)).toBe(expected);
  });
});
