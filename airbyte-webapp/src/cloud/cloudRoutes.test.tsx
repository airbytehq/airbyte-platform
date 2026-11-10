import { render, screen } from "@testing-library/react";
import React, { Suspense } from "react";
import { MemoryRouter, Outlet } from "react-router-dom";

import { useAcceptUserInvitation, useCurrentOrganizationInfo } from "core/api";

import { Routing } from "./cloudRoutes";
import { OnboardingGuard } from "./views/onboardingFlow/OnboardingGuard";

jest.mock("components/ui/LoadingPage", () => () => <div data-testid="loading-route" />);
jest.mock("area/layout/MainLayout", () => {
  const { Outlet } = jest.requireActual<typeof import("react-router-dom")>("react-router-dom");
  return () => (
    <div data-testid="main-layout">
      <Outlet />
    </div>
  );
});
jest.mock("area/workspace/utils", () => ({ useCurrentWorkspaceId: jest.fn(() => undefined) }));
jest.mock("core/api", () => ({
  useAcceptUserInvitation: jest.fn(() => ({ scopeType: "workspace", scopeId: "workspace-123" })),
  useCurrentOrganizationInfo: jest.fn(() => undefined),
  useCurrentWorkspaceOrUndefined: jest.fn(() => undefined),
  useInvalidateAllWorkspaceScopeOnChange: jest.fn(),
}));
jest.mock("core/services/analytics/useAnalyticsService", () => ({
  useAnalyticsIdentifyUser: jest.fn(),
  useAnalyticsRegisterValues: jest.fn(),
}));
jest.mock("core/services/auth", () => ({
  useAuthService: jest.fn(() => ({
    user: { userId: "invitee", email: "invitee@airbyte.test", name: "Invitee" },
    inited: true,
    provider: "keycloak",
    loggedOut: false,
  })),
}));
jest.mock("core/utils/connectorChatBuilderStorage", () => ({ storeConnectorChatBuilderFromQuery: jest.fn() }));
jest.mock("core/utils/freeEmailProviders", () => ({ isCorporateEmail: jest.fn(() => true) }));
jest.mock("core/utils/fullstory", () => ({
  fullStorySetIdentity: jest.fn(),
  fullStorySetUserProperties: jest.fn(),
}));
jest.mock("core/utils/useBuildUpdateCheck", () => ({ useBuildUpdateCheck: jest.fn() }));
jest.mock("core/utils/useLocalStorage", () => ({ useLocalStorage: jest.fn(() => ["", jest.fn()]) }));
jest.mock("core/utils/useOrganizationSubscriptionStatus", () => ({
  useOrganizationSubscriptionStatus: jest.fn(() => ({})),
}));
jest.mock("core/utils/useQuery", () => ({ useQuery: jest.fn(() => ({})) }));
jest.mock("core/utils/utmStorage", () => ({ storeUtmFromQuery: jest.fn() }));
jest.mock("pages/organization/OrganizationRoutes", () => ({ OrganizationRoutes: () => <div /> }));
jest.mock("./components/EntitlementsLoader", () => ({
  EntitlementsLoader: ({ children }: React.PropsWithChildren) => <>{children}</>,
}));
jest.mock("./services/thirdParty/fullstoryGuides/FullStoryGuidesProvider", () => ({
  useFullStoryGuidesReady: jest.fn(() => false),
}));
jest.mock("./services/thirdParty/launchdarkly", () => ({
  LDExperimentServiceProvider: ({ children }: React.PropsWithChildren) => <>{children}</>,
}));
jest.mock("./views/auth/SSOBookmarkPage", () => ({ SSOBookmarkPage: () => <div /> }));
jest.mock("./views/auth/SSOIdentifierPage", () => ({ SSOIdentifierPage: () => <div /> }));
jest.mock("./views/onboardingFlow/OnboardingGuard", () => ({ OnboardingGuard: jest.fn() }));
jest.mock("./views/routes/WorkspacesRoutes", () => ({
  WorkspacesRoutes: () => <div data-testid="workspace-route" />,
}));

const mockUseAcceptUserInvitation = useAcceptUserInvitation as jest.MockedFunction<typeof useAcceptUserInvitation>;
const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.MockedFunction<
  typeof useCurrentOrganizationInfo
>;
const mockOnboardingGuard = jest.mocked(OnboardingGuard);

beforeEach(() => {
  mockOnboardingGuard.mockImplementation(() => <Outlet />);
});

describe("Cloud invitation routing", () => {
  it("accepts an invitation when the FullStory hook has no current organization", async () => {
    render(
      <MemoryRouter initialEntries={["/accept-invite?inviteCode=invite-123"]}>
        <Suspense fallback={<div data-testid="outer-suspense" />}>
          <Routing />
        </Suspense>
      </MemoryRouter>
    );

    expect(mockUseCurrentOrganizationInfo).toHaveBeenCalled();
    expect(mockUseAcceptUserInvitation).toHaveBeenCalledWith("invite-123");
    expect(await screen.findByTestId("workspace-route")).toBeInTheDocument();
    expect(screen.queryByTestId("loading-route")).not.toBeInTheDocument();
  });
});

describe("Cloud main app routing", () => {
  it("does not render the main layout while the onboarding guard holds the route", () => {
    mockOnboardingGuard.mockImplementation(() => <div data-testid="onboarding-guard" />);

    render(
      <MemoryRouter initialEntries={["/workspaces/workspace-123/connections"]}>
        <Routing />
      </MemoryRouter>
    );

    expect(screen.getByTestId("onboarding-guard")).toBeInTheDocument();
    expect(screen.queryByTestId("main-layout")).not.toBeInTheDocument();
  });
});
