import { mockSource } from "test-utils";

import { getDraftChanges, getSourcePicksFromDrafts } from "./draftConnectors";

interface Draft {
  id: string;
  definitionId: string;
}

const getDefinitionId = ({ definitionId }: Draft) => definitionId;

describe("getDraftChanges", () => {
  it("creates the new picks and deletes the drafts that are no longer picked", () => {
    const drafts = [
      { id: "draft-shopify", definitionId: "shopify" },
      { id: "draft-stripe", definitionId: "stripe" },
    ];

    expect(getDraftChanges(["shopify", "klaviyo", "hubspot"], drafts, getDefinitionId)).toEqual({
      definitionIdsToCreate: ["klaviyo", "hubspot"],
      draftsToDelete: [{ id: "draft-stripe", definitionId: "stripe" }],
    });
  });

  it("creates every pick when no draft exists", () => {
    expect(getDraftChanges(["shopify", "klaviyo"], [], getDefinitionId)).toEqual({
      definitionIdsToCreate: ["shopify", "klaviyo"],
      draftsToDelete: [],
    });
  });

  it("changes nothing when the drafts already match the picks", () => {
    const drafts = [{ id: "draft-bigquery", definitionId: "bigquery" }];

    expect(getDraftChanges(["bigquery"], drafts, getDefinitionId)).toEqual({
      definitionIdsToCreate: [],
      draftsToDelete: [],
    });
  });
});

describe("getSourcePicksFromDrafts", () => {
  it("returns the picks in creation order, and keeps the list order for drafts created in the same second", () => {
    const drafts = [
      { ...mockSource, sourceDefinitionId: "hubspot", createdAt: 20 },
      { ...mockSource, sourceDefinitionId: "shopify", createdAt: 10 },
      { ...mockSource, sourceDefinitionId: "stripe", createdAt: 20 },
    ];

    expect(getSourcePicksFromDrafts(drafts)).toEqual(["shopify", "hubspot", "stripe"]);
  });
});
