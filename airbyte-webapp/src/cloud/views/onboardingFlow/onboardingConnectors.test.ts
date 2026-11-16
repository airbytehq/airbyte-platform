import { mockDestination } from "test-utils";

import { ConnectorIds } from "area/connector/utils";

import { getDestinationChoice } from "./onboardingConnectors";

describe("getDestinationChoice", () => {
  it("keeps a ready unlisted destination as the choice after a reload when no listed destination exists", () => {
    const unlistedDestination = {
      ...mockDestination,
      destinationDefinitionId: ConnectorIds.Destinations.EndToEndTesting,
    };

    expect(getDestinationChoice([], [unlistedDestination])).toBe(ConnectorIds.Destinations.EndToEndTesting);
  });
});
