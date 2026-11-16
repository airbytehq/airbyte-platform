import { mockDestination, mockSource } from "test-utils";

import { DestinationRead } from "core/api/types/AirbyteClient";

import { getDraftChanges, getSourcePicks, getStepDestination } from "./draftConnectors";

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

    expect(
      getDraftChanges(["shopify", "klaviyo", "hubspot"], { drafts, readyConnectors: [] }, getDefinitionId)
    ).toEqual({
      definitionIdsToCreate: ["klaviyo", "hubspot"],
      draftsToDelete: [{ id: "draft-stripe", definitionId: "stripe" }],
    });
  });

  it("creates every pick when no draft exists", () => {
    expect(getDraftChanges(["shopify", "klaviyo"], { drafts: [], readyConnectors: [] }, getDefinitionId)).toEqual({
      definitionIdsToCreate: ["shopify", "klaviyo"],
      draftsToDelete: [],
    });
  });

  it("changes nothing when the drafts already match the picks", () => {
    const drafts = [{ id: "draft-bigquery", definitionId: "bigquery" }];

    expect(getDraftChanges(["bigquery"], { drafts, readyConnectors: [] }, getDefinitionId)).toEqual({
      definitionIdsToCreate: [],
      draftsToDelete: [],
    });
  });

  it("creates no draft for a pick that is already set up, and never deletes a set-up connector", () => {
    const readyConnectors = [
      { id: "ready-shopify", definitionId: "shopify" },
      { id: "ready-hubspot", definitionId: "hubspot" },
    ];

    expect(getDraftChanges(["shopify", "stripe"], { drafts: [], readyConnectors }, getDefinitionId)).toEqual({
      definitionIdsToCreate: ["stripe"],
      draftsToDelete: [],
    });
  });
});

describe("getSourcePicks", () => {
  it("returns the picks in creation order, and keeps the list order for drafts created in the same second", () => {
    const drafts = [
      { ...mockSource, sourceDefinitionId: "hubspot", createdAt: 20 },
      { ...mockSource, sourceDefinitionId: "shopify", createdAt: 10 },
      { ...mockSource, sourceDefinitionId: "stripe", createdAt: 20 },
    ];

    expect(getSourcePicks(drafts, [])).toEqual(["shopify", "hubspot", "stripe"]);
  });
});

describe("getStepDestination", () => {
  const draft = { ...mockDestination, destinationId: "draft", destinationDefinitionId: "snowflake", isDraft: true };
  const isSnowflake = ({ destinationDefinitionId }: DestinationRead) => destinationDefinitionId === "snowflake";

  it("opens the ready destination of the choice when the step already passed its test", () => {
    const ready = { ...draft, destinationId: "ready", isDraft: false };

    expect(getStepDestination([ready], draft, isSnowflake)).toBe(ready);
  });

  it("opens the draft of the choice when the ready destination is another definition", () => {
    const readyPostgres = { ...draft, destinationId: "ready", destinationDefinitionId: "postgres", isDraft: false };

    expect(getStepDestination([readyPostgres], draft, isSnowflake)).toBe(draft);
  });

  it("opens the draft when no destination is ready", () => {
    expect(getStepDestination([], draft, isSnowflake)).toBe(draft);
  });
});
