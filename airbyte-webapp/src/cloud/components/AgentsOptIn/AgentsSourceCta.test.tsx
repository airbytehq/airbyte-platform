import { render, screen } from "@testing-library/react";
import { ComponentProps } from "react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useAgentsProvisioningStatus, useAgentsSupportedSourceDefinitions } from "core/api";
import { useIsCloudApp } from "core/utils/app";

import { AgentsSourceCta } from "./AgentsSourceCta";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useAgentsSupportedSourceDefinitions: jest.fn(),
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
const mockUseAgentsSupportedSourceDefinitions = useAgentsSupportedSourceDefinitions as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitions
>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

const messages = {
  "cloud.agentsOptIn.tryWithAgents": "Try with Agents",
};

const renderCta = (props: ComponentProps<typeof AgentsSourceCta>) =>
  render(
    <IntlProvider locale="en" messages={messages}>
      <MemoryRouter>
        <AgentsSourceCta {...props} />
      </MemoryRouter>
    </IntlProvider>
  );

describe("AgentsSourceCta", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-org-123");
    mockUseAgentsProvisioningStatus.mockReturnValue(null);
    mockUseAgentsSupportedSourceDefinitions.mockReturnValue(new Set(["GitHub"]));
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
  });

  it("renders for a supported source", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: "not_provisioned",
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-org-123",
    });
    renderCta({ actorType: "source", actorDefinitionName: "GitHub" });

    expect(screen.getByRole("button", { name: "Try with Agents" })).toBeInTheDocument();
  });

  it("does not render when provisioning status is unavailable", () => {
    renderCta({ actorType: "source", actorDefinitionName: "GitHub" });

    expect(screen.queryByRole("button", { name: "Try with Agents" })).not.toBeInTheDocument();
  });

  it("does not render when the organization is not eligible", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: null,
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: false,
      eligible_external_organization_id: null,
    });
    renderCta({ actorType: "source", actorDefinitionName: "GitHub" });

    expect(screen.queryByRole("button", { name: "Try with Agents" })).not.toBeInTheDocument();
  });

  it("does not render for an unsupported source", () => {
    renderCta({ actorType: "source", actorDefinitionName: "Not Supported" });

    expect(screen.queryByRole("button", { name: "Try with Agents" })).not.toBeInTheDocument();
  });

  it("does not render for a destination", () => {
    renderCta({ actorType: "destination", actorDefinitionName: "GitHub" });

    expect(screen.queryByRole("button", { name: "Try with Agents" })).not.toBeInTheDocument();
  });

  it("does not render when the Agents opt-in gate is disabled", () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);

    renderCta({ actorType: "source", actorDefinitionName: "GitHub" });

    expect(screen.queryByRole("button", { name: "Try with Agents" })).not.toBeInTheDocument();
  });
});
