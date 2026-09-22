import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
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
  "cloud.agentsOptIn.banner": "Your organization can now use Airbyte Agents. <lnk>Get started</lnk>",
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
});
