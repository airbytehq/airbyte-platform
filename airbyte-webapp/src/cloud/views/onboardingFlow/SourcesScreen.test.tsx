import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { TestWrapper } from "test-utils/testutils";

import { SourceDefinitionRead } from "core/api/types/AirbyteClient";

import { SourcesScreen } from "./SourcesScreen";

const mockTrack = jest.fn();
const mockCreateSource = jest.fn();

const SOURCE_DEFINITIONS = [
  { sourceDefinitionId: "definition-a", name: "Source A" },
  { sourceDefinitionId: "definition-b", name: "Source B" },
] as SourceDefinitionRead[];

jest.mock("core/services/analytics", () => ({
  ...jest.requireActual("core/services/analytics"),
  useAnalyticsService: () => ({ track: mockTrack }),
}));
jest.mock("core/api/generated/AirbyteClient", () => ({
  ...jest.requireActual("core/api/generated/AirbyteClient"),
  createSource: (...args: unknown[]) => mockCreateSource(...args),
}));
jest.mock("core/api/useRequestOptions", () => ({ useRequestOptions: () => ({}) }));
jest.mock("core/api/hooks/workspaces", () => ({
  ...jest.requireActual("core/api/hooks/workspaces"),
  useCurrentWorkspace: () => ({ workspaceId: "workspace-1" }),
}));
/** The real source create hook runs, so the test sees whether a draft create sends a Segment event. */
jest.mock("core/api", () => ({
  ...jest.requireActual("core/api/hooks/sources"),
  useSourceDefinitionList: () => ({
    sourceDefinitions: SOURCE_DEFINITIONS,
    sourceDefinitionMap: new Map(SOURCE_DEFINITIONS.map((definition) => [definition.sourceDefinitionId, definition])),
  }),
  useDraftSourceList: () => [],
}));

describe("SourcesScreen", () => {
  it("sends the ordered picks as an onboarding answer and no source event when Continue saves the drafts", async () => {
    mockCreateSource.mockResolvedValue({});
    const onContinue = jest.fn();

    render(
      <TestWrapper>
        <SourcesScreen
          picks={["definition-b", "definition-a"]}
          onPicksChange={jest.fn()}
          onBack={jest.fn()}
          onContinue={onContinue}
        />
      </TestWrapper>
    );

    await userEvent.click(screen.getByRole("button", { name: "Continue" }));

    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect(mockCreateSource).toHaveBeenCalledTimes(2);
    expect(mockTrack).toHaveBeenCalledTimes(1);
    expect(mockTrack).toHaveBeenCalledWith("Onboarding", "Answered", {
      actionDescription: "Onboarding sources answered",
      screen_id: "sources",
      selection_order: ["definition-b", "definition-a"],
    });
  });
});
