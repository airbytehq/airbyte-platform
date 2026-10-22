import { fireEvent, screen } from "@testing-library/react";

import { render } from "test-utils";

import { useIsCloudApp } from "core/utils/app";

import { InstallMcpPage } from "./InstallMcpPage";
import { buildCursorDeeplink, CLOUD_MCP_URL } from "./mcpInstallConfigs";

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("core/utils/links", () => ({
  links: {
    airbyteMcpDocs: "https://docs.airbyte.com/platform/airbyte-mcp",
  },
}));

const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;

describe("InstallMcpPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseIsCloudApp.mockReturnValue(true);
  });

  it("renders the install page and cloud MCP URL", async () => {
    await render(<InstallMcpPage />);

    expect(screen.getByRole("heading", { name: "Install MCP" })).toBeInTheDocument();
    expect(screen.getAllByText(CLOUD_MCP_URL, { exact: false }).length).toBeGreaterThan(0);
    expect(screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent)).toEqual([
      "Paste a URL",
      "One click install",
      "CLI command",
      "JSON config file",
    ]);
    expect(screen.getAllByRole("button", { name: "Copy" })).toHaveLength(6);
    expect(screen.getByText(/codex mcp add airbyte/)).toBeInTheDocument();
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

  it("renders nothing outside the Cloud app", async () => {
    mockUseIsCloudApp.mockReturnValue(false);

    const { container } = await render(<InstallMcpPage />);

    expect(screen.queryByRole("heading", { name: "Install MCP" })).not.toBeInTheDocument();
    expect(container.firstChild).toBeEmptyDOMElement();
  });
});
