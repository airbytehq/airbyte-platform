import { fireEvent, screen } from "@testing-library/react";

import { render } from "test-utils";

import { useAgentsProvisioningStatus } from "core/api";
import { useIsCloudApp } from "core/utils/app";

import { InstallMcpPage } from "./InstallMcpPage";
import { buildCursorDeeplink, CLOUD_MCP_URL } from "./mcpInstallConfigs";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("core/utils/links", () => ({
  links: {
    agentsDocs: "https://docs.airbyte.com/ai-agents/get-started",
  },
}));

jest.mock("./useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

const mockUseAgentsProvisioningStatus = useAgentsProvisioningStatus as jest.MockedFunction<
  typeof useAgentsProvisioningStatus
>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

describe("InstallMcpPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      external_cloud_eligible: true,
    } as never);
  });

  it("renders the install page and cloud MCP URL", async () => {
    await render(<InstallMcpPage />);

    expect(screen.getByRole("heading", { name: "Install MCP" })).toBeInTheDocument();
    expect(screen.getByDisplayValue(CLOUD_MCP_URL)).toBeInTheDocument();
    expect(screen.getByText(/"servers":/)).toBeInTheDocument();
    expect(screen.getByText(/"mcpServers":/)).toBeInTheDocument();
  });

  it("opens the Cursor install deeplink", async () => {
    const open = jest.spyOn(window, "open").mockImplementation(() => null);

    await render(<InstallMcpPage />);
    fireEvent.click(screen.getByTestId("installMcp-cursor"));

    expect(open).toHaveBeenCalledWith(buildCursorDeeplink(CLOUD_MCP_URL), "_blank");
    open.mockRestore();
  });

  it("renders nothing when the agents opt-in is disabled", async () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);

    const { container } = await render(<InstallMcpPage />);

    expect(screen.queryByRole("heading", { name: "Install MCP" })).not.toBeInTheDocument();
    expect(container.firstChild).toBeEmptyDOMElement();
  });
});
