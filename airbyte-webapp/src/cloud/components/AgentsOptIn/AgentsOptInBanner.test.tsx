import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
import { BannerGroupContext } from "cloud/components/CloudBannerGroup/BannerGroupContext";
import { useAgentsProvisioningStatus } from "core/api";
import { useIsCloudApp } from "core/utils/app";

import { AgentsOptInBanner } from "./AgentsOptInBanner";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
}));

jest.mock("core/api/cloud", () => ({
  useFusionUsage: () => ({ isEligible: false }),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("./useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseAgentsProvisioningStatus = useAgentsProvisioningStatus as jest.MockedFunction<
  typeof useAgentsProvisioningStatus
>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

const messages = {
  "cloud.agentsOptIn.banner": "Your organization can now use Airbyte Cloud's Context Layer. <lnk>Get started</lnk>.",
};

describe("AgentsOptInBanner", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-org-123");
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      external_cloud_eligible: true,
    } as never);
  });

  it("links eligible organizations to the first-class Context Layer route", () => {
    render(
      <IntlProvider locale="en" messages={messages}>
        <MemoryRouter>
          <AgentsOptInBanner />
        </MemoryRouter>
      </IntlProvider>
    );

    expect(screen.getByRole("link", { name: "Get started" })).toHaveAttribute(
      "href",
      "/organization/test-org-123/context-layer"
    );
  });
  describe("registration with the banner group", () => {
    const renderInGroup = () => {
      const registerBanner = jest.fn();
      render(
        <IntlProvider locale="en" messages={messages}>
          <MemoryRouter>
            <BannerGroupContext.Provider
              value={{ registerBanner, unregisterBanner: jest.fn(), checkIsBannerHidden: () => false }}
            >
              <AgentsOptInBanner />
            </BannerGroupContext.Provider>
          </MemoryRouter>
        </IntlProvider>
      );

      return registerBanner;
    };

    it("should register when the organization is eligible", () => {
      const registerBanner = renderInGroup();

      expect(registerBanner).toHaveBeenCalledWith("agents-opt-in");
    });

    it("should register only once", () => {
      const registerBanner = renderInGroup();

      expect(registerBanner).toHaveBeenCalledTimes(1);
    });

    it.each([
      ["already enrolled", { is_enrolled: true, external_cloud_eligible: true }],
      ["not eligible", { is_enrolled: false, external_cloud_eligible: false }],
      ["no provisioning status", undefined],
    ])("should register nothing when %s", (_name, status) => {
      mockUseAgentsProvisioningStatus.mockReturnValue(status as never);

      const registerBanner = renderInGroup();

      expect(registerBanner).not.toHaveBeenCalled();
    });

    it("should register nothing when the opt-in is not shown for this organization", () => {
      mockUseShowAgentsOptIn.mockReturnValue(false);

      const registerBanner = renderInGroup();

      expect(registerBanner).not.toHaveBeenCalled();
    });
  });
});
