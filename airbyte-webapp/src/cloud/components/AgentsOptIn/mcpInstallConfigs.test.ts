import {
  buildClaudeCodeCommand,
  buildCursorConfig,
  buildCursorDeeplink,
  buildVsCodeCliCommand,
  buildVsCodeConfig,
  buildVsCodeDeeplink,
  buildVsCodeInsidersDeeplink,
  CLOUD_MCP_URL,
} from "./mcpInstallConfigs";

const TEST_URL = "https://mcp.example.com/mcp";

const unwrapVsCodeRedirect = (installUrl: string, redirectHost: string): string => {
  const prefix = `https://${redirectHost}/redirect?url=`;
  expect(installUrl.startsWith(prefix)).toBe(true);
  return decodeURIComponent(installUrl.slice(prefix.length));
};

describe("mcpInstallConfigs", () => {
  it("exports the cloud MCP server URL", () => {
    expect(CLOUD_MCP_URL).toBe("https://mcp.internal.airbyte.ai/cloud-mcp-preview");
  });

  it("builds a Cursor deeplink with base64-encoded config and server name", () => {
    const deeplink = buildCursorDeeplink(TEST_URL);
    expect(deeplink.startsWith("cursor://anysphere.cursor-deeplink/mcp/install?")).toBe(true);
    expect(deeplink).toContain("name=airbyte");
    const configParam = new URL(deeplink.replace("cursor://", "http://cursor/")).searchParams.get("config");
    expect(configParam).not.toBeNull();
    const decoded = JSON.parse(atob(configParam!));
    expect(decoded).toEqual({ url: TEST_URL });
  });

  it.each([
    { variant: "stable" as const, build: buildVsCodeDeeplink, scheme: "vscode", redirectHost: "vscode.dev" },
    {
      variant: "insiders" as const,
      build: buildVsCodeInsidersDeeplink,
      scheme: "vscode-insiders",
      redirectHost: "insiders.vscode.dev",
    },
  ])(
    "builds a VS Code $variant install URL via the redirect wrapper with a flat payload",
    ({ build, scheme, redirectHost }) => {
      const installUrl = build(TEST_URL);
      const deeplink = unwrapVsCodeRedirect(installUrl, redirectHost);

      expect(deeplink.startsWith(`${scheme}:mcp/install?`)).toBe(true);
      expect(deeplink.startsWith(`${scheme}://`)).toBe(false);

      const encoded = deeplink.slice(`${scheme}:mcp/install?`.length);
      const decoded = JSON.parse(decodeURIComponent(encoded));
      expect(decoded).toEqual({ name: "airbyte", type: "http", url: TEST_URL });
      expect(decoded).not.toHaveProperty("config");
    }
  );

  it("builds the Claude Code CLI command", () => {
    expect(buildClaudeCodeCommand(TEST_URL)).toBe(`claude mcp add --transport http airbyte ${TEST_URL}`);
  });

  it("builds the Cursor JSON config", () => {
    expect(buildCursorConfig(TEST_URL)).toBe(JSON.stringify({ mcpServers: { airbyte: { url: TEST_URL } } }, null, 2));
  });

  it("builds the VS Code JSON config", () => {
    expect(buildVsCodeConfig(TEST_URL)).toBe(
      JSON.stringify({ servers: { airbyte: { type: "http", url: TEST_URL } } }, null, 2)
    );
  });

  it("builds the VS Code CLI command", () => {
    expect(buildVsCodeCliCommand(TEST_URL)).toBe(
      `code --add-mcp '${JSON.stringify({ name: "airbyte", type: "http", url: TEST_URL })}'`
    );
  });
});
