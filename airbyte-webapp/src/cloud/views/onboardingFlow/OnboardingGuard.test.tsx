import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router-dom";

import { useDefaultWorkspaceInOrganization } from "core/api";
import { WorkspaceRead } from "core/api/types/AirbyteClient";

import { OnboardingGuard } from "./OnboardingGuard";

jest.mock("area/organization/utils", () => ({ useCurrentOrganizationId: () => "org-1" }));
jest.mock("core/api", () => ({ useDefaultWorkspaceInOrganization: jest.fn() }));
jest.mock("./useShouldEnterOnboardingFlow", () => ({ useShouldEnterOnboardingFlow: () => true }));

const mockUseDefaultWorkspaceInOrganization = useDefaultWorkspaceInOrganization as jest.MockedFunction<
  typeof useDefaultWorkspaceInOrganization
>;

const OnboardingFlowRoute = () => <div data-testid="onboarding-flow">{useParams().workspaceId}</div>;

const renderOrganizationPage = () =>
  render(
    <MemoryRouter initialEntries={["/organization/org-1/workspaces"]}>
      <Routes>
        <Route element={<OnboardingGuard />}>
          <Route path="organization/:organizationId/*" element={<div data-testid="organization-page" />} />
        </Route>
        <Route path="workspaces/:workspaceId/onboarding-flow" element={<OnboardingFlowRoute />} />
      </Routes>
    </MemoryRouter>
  );

describe("OnboardingGuard on an organization page", () => {
  it("sends the user into the flow in the organization's default workspace", () => {
    mockUseDefaultWorkspaceInOrganization.mockReturnValue({ workspaceId: "workspace-1" } as WorkspaceRead);

    renderOrganizationPage();

    expect(mockUseDefaultWorkspaceInOrganization).toHaveBeenCalledWith("org-1", true);
    expect(screen.getByTestId("onboarding-flow")).toHaveTextContent("workspace-1");
  });

  it("shows the organization page when the organization has no workspace", () => {
    mockUseDefaultWorkspaceInOrganization.mockReturnValue(undefined);

    renderOrganizationPage();

    expect(screen.getByTestId("organization-page")).toBeInTheDocument();
  });
});
