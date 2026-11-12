import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";

import { TestWrapper } from "test-utils/testutils";

import { DestinationScreen } from "./DestinationScreen";

const mockUpdateOnboarding = jest.fn();
const mockFetchOrganizationInfo = jest.fn();

jest.mock("area/organization/utils", () => ({ useCurrentOrganizationId: () => "org-1" }));
jest.mock("area/workspace/utils", () => ({ useCurrentWorkspaceId: () => "workspace-1" }));
jest.mock("core/api", () => ({
  useDestinationDefinitionList: () => ({ destinationDefinitionMap: new Map() }),
  useFetchOrganizationInfo: () => mockFetchOrganizationInfo,
  useUpdateOrganizationOnboarding: () => ({ mutate: mockUpdateOnboarding, isLoading: false }),
}));
jest.mock("./useSyncDraftConnectors", () => ({
  isMissingDefinitionError: () => false,
  useSyncDraftDestination: () => async () => undefined,
}));

describe("DestinationScreen", () => {
  it("hides the not sure option when the recommended destination is not available", () => {
    render(
      <TestWrapper>
        <DestinationScreen onChoiceChange={jest.fn()} onBack={jest.fn()} onContinue={jest.fn()} />
      </TestWrapper>
    );

    expect(screen.queryByText("Not sure yet")).not.toBeInTheDocument();
  });

  it("opens the normal app and saves nothing when onboarding was finished in another tab or device meanwhile", async () => {
    mockFetchOrganizationInfo.mockResolvedValue({ onboardingStatus: "skipped" });
    const onContinue = jest.fn();

    render(
      <TestWrapper route="/workspaces/workspace-1/onboarding-flow">
        <Routes>
          <Route
            path="workspaces/:workspaceId/onboarding-flow"
            element={
              <DestinationScreen
                choice="destination-1"
                onChoiceChange={jest.fn()}
                onBack={jest.fn()}
                onContinue={onContinue}
              />
            }
          />
          <Route path="workspaces/:workspaceId" element={<div data-testid="workspace-page" />} />
        </Routes>
      </TestWrapper>
    );

    await userEvent.click(screen.getByRole("button", { name: "Continue" }));

    expect(await screen.findByTestId("workspace-page")).toBeInTheDocument();
    expect(mockUpdateOnboarding).not.toHaveBeenCalled();
    expect(onContinue).not.toHaveBeenCalled();
  });
});
