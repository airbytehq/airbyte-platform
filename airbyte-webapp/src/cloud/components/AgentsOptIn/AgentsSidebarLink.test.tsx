import { render, screen } from "@testing-library/react";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useAgentsProvisioningStatus } from "core/api";
import { useGeneratedIntent } from "core/utils/rbac";

import { AgentsSidebarLink, InstallMcpSidebarLink } from "./AgentsSidebarLink";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/layout/SideBar/components/NavItem", () => ({
  NavItem: ({ to, testId }: { to: string; testId: string }) => (
    <a href={to} data-testid={testId}>
      {testId}
    </a>
  ),
}));
jest.mock("area/organization/utils", () => ({ useCurrentOrganizationId: jest.fn(() => "test-org") }));
jest.mock("core/api", () => ({ useAgentsProvisioningStatus: jest.fn() }));
jest.mock("core/utils/app", () => ({ useIsCloudApp: () => true }));
jest.mock("core/utils/rbac", () => ({
  Intent: { UpdateOrganizationPermissions: "UpdateOrganizationPermissions" },
  useGeneratedIntent: jest.fn(),
}));
jest.mock("./useShowAgentsOptIn", () => ({ useShowAgentsOptIn: jest.fn() }));

const mockUseAgentsProvisioningStatus = useAgentsProvisioningStatus as jest.MockedFunction<
  typeof useAgentsProvisioningStatus
>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

describe("AgentsSidebarLink", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.mocked(useCurrentOrganizationId).mockReturnValue("test-org");
    mockUseGeneratedIntent.mockReturnValue(false);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: false, external_cloud_eligible: true } as never);
  });

  it("shows Context Layer to an eligible admin before enrollment", () => {
    mockUseGeneratedIntent.mockReturnValue(true);
    render(<AgentsSidebarLink />);
    expect(screen.getByTestId("agentsSidebarLink")).toHaveAttribute("href", "/organization/test-org/context-layer");
  });

  it("hides Context Layer from a non-admin before enrollment", () => {
    render(<AgentsSidebarLink />);
    expect(screen.queryByTestId("agentsSidebarLink")).not.toBeInTheDocument();
  });

  it("hides Context Layer before enrollment when the frontend opt-in is disabled", () => {
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(false);
    render(<AgentsSidebarLink />);
    expect(screen.queryByTestId("agentsSidebarLink")).not.toBeInTheDocument();
  });

  it("shows Context Layer to an enrolled viewer", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true, external_cloud_eligible: false } as never);
    mockUseShowAgentsOptIn.mockReturnValue(false);
    render(<AgentsSidebarLink />);
    expect(screen.getByTestId("agentsSidebarLink")).toBeInTheDocument();
    expect(mockUseAgentsProvisioningStatus).toHaveBeenCalledWith({ enabled: true });
  });

  it("shows Install MCP for an eligible unenrolled organization", () => {
    render(<InstallMcpSidebarLink />);
    expect(screen.getByTestId("installMcpSidebarLink")).toHaveAttribute("href", "/organization/test-org/install-mcp");
    expect(mockUseAgentsProvisioningStatus).toHaveBeenCalledWith({ enabled: true });
  });

  it("hides Install MCP for an ineligible unenrolled organization", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: false, external_cloud_eligible: false } as never);
    render(<InstallMcpSidebarLink />);
    expect(screen.queryByTestId("installMcpSidebarLink")).not.toBeInTheDocument();
  });

  it("hides Install MCP when the frontend opt-in is disabled", () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);
    render(<InstallMcpSidebarLink />);
    expect(screen.queryByTestId("installMcpSidebarLink")).not.toBeInTheDocument();
    expect(mockUseAgentsProvisioningStatus).toHaveBeenCalledWith({ enabled: false });
  });
});
