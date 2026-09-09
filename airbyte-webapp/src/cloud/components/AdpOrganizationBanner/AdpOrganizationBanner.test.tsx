import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useCurrentOrganizationId, useIsAdpOrganization } from "area/organization/utils";

import { AdpOrganizationBanner } from "./AdpOrganizationBanner";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
  useIsAdpOrganization: jest.fn(),
}));

const mockUseIsAdpOrganization = useIsAdpOrganization as jest.MockedFunction<typeof useIsAdpOrganization>;
const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;

const mockOrganizationId = "test-org-123";

const messages = {
  "cloud.adpOrganization.banner":
    "This is an Airbyte Agents organization. <lnk>Manage the Context Layer in organization settings</lnk>.",
};

const renderWithIntl = (component: React.ReactElement) => {
  return render(
    <IntlProvider locale="en" messages={messages}>
      <MemoryRouter>{component}</MemoryRouter>
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
    expect(screen.getByText(/This is an Airbyte Agents organization/)).toBeInTheDocument();
  });

  it("should not render banner when organization is not ADP", () => {
    mockUseIsAdpOrganization.mockReturnValue(false);

    renderWithIntl(<AdpOrganizationBanner />);

    expect(screen.queryByTestId("adp-organization-banner")).not.toBeInTheDocument();
  });

  it("should contain an internal Context Layer settings link with the correct organization ID", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);

    renderWithIntl(<AdpOrganizationBanner />);

    const link = screen.getByRole("link");
    expect(link).toHaveAttribute("href", `/organization/${mockOrganizationId}/settings/context-layer`);
  });

  it("should have correct data-testid attribute", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);

    renderWithIntl(<AdpOrganizationBanner />);

    expect(screen.getByTestId("adp-organization-banner")).toBeInTheDocument();
  });
});
