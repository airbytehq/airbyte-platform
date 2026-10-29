import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useCurrentOrganizationId, useIsAdpOrganization } from "area/organization/utils";
import { BannerGroupContext } from "cloud/components/CloudBannerGroup/BannerGroupContext";

import { AdpOrganizationBanner } from "./AdpOrganizationBanner";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
  useIsAdpOrganization: jest.fn(),
}));

const mockUseIsAdpOrganization = useIsAdpOrganization as jest.MockedFunction<typeof useIsAdpOrganization>;
const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;

const mockOrganizationId = "test-org-123";

const messages = {
  "cloud.adpOrganization.banner": "This is an Airbyte Agents organization. <lnk>Manage the Context Layer</lnk>.",
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
    expect(link).toHaveAttribute("href", `/organization/${mockOrganizationId}/context-layer`);
  });

  it("should have correct data-testid attribute", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);

    renderWithIntl(<AdpOrganizationBanner />);

    expect(screen.getByTestId("adp-organization-banner")).toBeInTheDocument();
  });
  describe("registration with the banner group", () => {
    const renderInGroup = (isBannerHidden = false) => {
      const registerBanner = jest.fn();
      renderWithIntl(
        <BannerGroupContext.Provider
          value={{ registerBanner, unregisterBanner: jest.fn(), checkIsBannerHidden: () => isBannerHidden }}
        >
          <AdpOrganizationBanner />
        </BannerGroupContext.Provider>
      );

      return registerBanner;
    };

    it("should register when the organization is ADP", () => {
      mockUseIsAdpOrganization.mockReturnValue(true);

      const registerBanner = renderInGroup();

      expect(registerBanner).toHaveBeenCalledWith("adp-organization");
    });

    it("should register only once", () => {
      mockUseIsAdpOrganization.mockReturnValue(true);

      const registerBanner = renderInGroup();

      expect(registerBanner).toHaveBeenCalledTimes(1);
    });

    it("should register nothing when the organization is not ADP", () => {
      mockUseIsAdpOrganization.mockReturnValue(false);

      const registerBanner = renderInGroup();

      expect(registerBanner).not.toHaveBeenCalled();
    });

    it("should not render while the group hides it", () => {
      mockUseIsAdpOrganization.mockReturnValue(true);

      renderInGroup(true);

      expect(screen.queryByTestId("adp-organization-banner")).not.toBeInTheDocument();
    });

    it("should stay registered while the group hides it", () => {
      mockUseIsAdpOrganization.mockReturnValue(true);

      const registerBanner = renderInGroup(true);

      expect(registerBanner).toHaveBeenCalledWith("adp-organization");
    });
  });
});
