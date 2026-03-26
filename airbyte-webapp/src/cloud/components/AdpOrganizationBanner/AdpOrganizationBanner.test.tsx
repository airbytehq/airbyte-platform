import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { useCurrentOrganizationId, useIsAdpOrganization } from "area/organization/utils";

import { AdpOrganizationBanner } from "./AdpOrganizationBanner";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
  useIsAdpOrganization: jest.fn(),
}));

jest.mock("core/utils/links", () => ({
  links: {
    agentEngineApp: "https://app.airbyte.ai",
  },
}));

const mockUseIsAdpOrganization = useIsAdpOrganization as jest.MockedFunction<typeof useIsAdpOrganization>;
const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;

const mockOrganizationId = "test-org-123";

const messages = {
  "cloud.adpOrganization.banner": "This is an Agent Engine organization. <lnk>Manage it on app.airbyte.ai</lnk>.",
};

const renderWithIntl = (component: React.ReactElement) => {
  return render(
    <IntlProvider locale="en" messages={messages}>
      {component}
    </IntlProvider>
  );
};

describe("AdpOrganizationBanner", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue(mockOrganizationId);
  });

  it("should render banner when organization is ADP", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);

    renderWithIntl(<AdpOrganizationBanner />);

    expect(screen.getByTestId("adp-organization-banner")).toBeInTheDocument();
    expect(screen.getByText(/This is an Agent Engine organization/)).toBeInTheDocument();
  });

  it("should not render banner when organization is not ADP", () => {
    mockUseIsAdpOrganization.mockReturnValue(false);

    renderWithIntl(<AdpOrganizationBanner />);

    expect(screen.queryByTestId("adp-organization-banner")).not.toBeInTheDocument();
  });

  it("should contain external link with correct organization ID in URL", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);

    renderWithIntl(<AdpOrganizationBanner />);

    const link = screen.getByRole("link");
    expect(link).toHaveAttribute("href", `https://app.airbyte.ai/organizations/${mockOrganizationId}`);
  });

  it("should have correct data-testid attribute", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);

    renderWithIntl(<AdpOrganizationBanner />);

    expect(screen.getByTestId("adp-organization-banner")).toBeInTheDocument();
  });
});
