import { render, screen } from "@testing-library/react";

import { useCurrentOrganizationInfo, useCurrentWorkspaceOrUndefined } from "core/api";
import { useAuthService } from "core/services/auth";

import { FullStoryUserProperties } from "./FullStoryUserProperties";
import { useFullStoryUserProperties } from "./useFullStoryUserProperties";

jest.mock("core/api", () => ({
  useCurrentOrganizationInfo: jest.fn(() => undefined),
  useCurrentWorkspaceOrUndefined: jest.fn(() => undefined),
}));
jest.mock("core/services/auth", () => ({
  useAuthService: jest.fn(() => ({
    authType: "cloud",
    applicationSupport: "multiple",
    user: null,
    inited: true,
    emailVerified: true,
    loggedOut: false,
    provider: "keycloak",
  })),
}));
jest.mock("./useFullStoryUserProperties", () => ({
  useFullStoryUserProperties: jest.fn(),
}));

const mockUseCurrentOrganizationInfo = jest.mocked(useCurrentOrganizationInfo);
const mockUseCurrentWorkspaceOrUndefined = jest.mocked(useCurrentWorkspaceOrUndefined);
const mockUseAuthService = jest.mocked(useAuthService);
const mockUseFullStoryUserProperties = jest.mocked(useFullStoryUserProperties);

describe("FullStoryUserProperties", () => {
  beforeEach(() => {
    mockUseCurrentOrganizationInfo.mockReturnValue(undefined);
    mockUseCurrentWorkspaceOrUndefined.mockReturnValue(undefined);
    mockUseFullStoryUserProperties.mockReset();
    mockUseAuthService.mockReturnValue({
      authType: "cloud",
      applicationSupport: "multiple",
      user: null,
      inited: true,
      emailVerified: true,
      loggedOut: false,
      provider: "keycloak",
    });
  });

  it("keeps sibling content rendered while the FullStory sync suspends", () => {
    mockUseFullStoryUserProperties.mockImplementation(() => {
      throw new Promise(() => undefined);
    });

    render(
      <>
        <FullStoryUserProperties />
        <div data-testid="sibling-content">Sibling content</div>
      </>
    );

    expect(screen.getByTestId("sibling-content")).toBeInTheDocument();
  });

  it("renders nothing when the FullStory sync hook throws", () => {
    mockUseFullStoryUserProperties.mockImplementation(() => {
      throw new Error("FullStory sync failed");
    });

    const { container } = render(<FullStoryUserProperties />);

    expect(container).toBeEmptyDOMElement();
  });
});
