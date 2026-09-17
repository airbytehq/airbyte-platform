const MCP_SERVER_NAME = "airbyte";

// Preview URL, swapped for prod later.
export const CLOUD_MCP_URL = "https://mcp.internal.airbyte.ai/cloud-mcp-preview";

export const buildCursorDeeplink = (url: string): string => {
  const config = { url };
  const base64 = btoa(JSON.stringify(config));
  const name = encodeURIComponent(MCP_SERVER_NAME);
  return `cursor://anysphere.cursor-deeplink/mcp/install?name=${name}&config=${encodeURIComponent(base64)}`;
};

type VsCodeVariant = "stable" | "insiders";

const VS_CODE_VARIANT_CONFIG: Record<VsCodeVariant, { scheme: string; redirectHost: string }> = {
  stable: { scheme: "vscode", redirectHost: "vscode.dev" },
  insiders: { scheme: "vscode-insiders", redirectHost: "insiders.vscode.dev" },
};

const buildVsCodeInstallUrl = (url: string, variant: VsCodeVariant): string => {
  const { scheme, redirectHost } = VS_CODE_VARIANT_CONFIG[variant];
  const payload = { name: MCP_SERVER_NAME, type: "http", url };
  const deeplink = `${scheme}:mcp/install?${encodeURIComponent(JSON.stringify(payload))}`;
  return `https://${redirectHost}/redirect?url=${encodeURIComponent(deeplink)}`;
};

export const buildVsCodeDeeplink = (url: string): string => buildVsCodeInstallUrl(url, "stable");

export const buildVsCodeInsidersDeeplink = (url: string): string => buildVsCodeInstallUrl(url, "insiders");

export const buildClaudeCodeCommand = (url: string): string =>
  `claude mcp add --transport http ${MCP_SERVER_NAME} ${url}`;

export const buildCursorConfig = (url: string): string =>
  JSON.stringify({ mcpServers: { [MCP_SERVER_NAME]: { url } } }, null, 2);

export const buildVsCodeConfig = (url: string): string =>
  JSON.stringify({ servers: { [MCP_SERVER_NAME]: { type: "http", url } } }, null, 2);

export const buildVsCodeCliCommand = (url: string): string =>
  `code --add-mcp '${JSON.stringify({ name: MCP_SERVER_NAME, type: "http", url })}'`;
