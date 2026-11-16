import { renderHook } from "@testing-library/react";

import { mockDestination, mockSource } from "test-utils";

import { useOnboardingPair } from "./useOnboardingPair";

const mockShopifySource = { ...mockSource, sourceId: "shopify-source", sourceDefinitionId: "shopify", isDraft: false };
const mockStripeSource = { ...mockSource, sourceId: "stripe-source", sourceDefinitionId: "stripe", isDraft: false };
const mockPostgresDestination = { ...mockDestination, destinationDefinitionId: "postgres", isDraft: false };

jest.mock("core/api", () => ({
  useFirstSourcePage: () => [mockShopifySource, mockStripeSource],
  useFirstDestinationPage: () => [mockPostgresDestination],
}));

describe("useOnboardingPair", () => {
  it("uses the ready source of a pick, not a ready source whose definition is no longer picked", () => {
    const { result } = renderHook(() => useOnboardingPair(["stripe"], "postgres"));

    expect(result.current).toEqual({ source: mockStripeSource, destination: mockPostgresDestination });
  });
});
