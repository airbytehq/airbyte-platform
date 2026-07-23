/**
 * Centralized tool name constants for the connector setup agent.
 * Use these constants instead of string literals to prevent typos and enable refactoring.
 */
export const TOOL_NAMES = {
  CHECK_CONFIGURATION: "check_configuration",
  GET_CONNECTOR_DOCUMENTATION: "get_connector_documentation",
  REQUEST_SECRET_INPUT: "request_secret_input",
  REQUEST_OAUTH_AUTHENTICATION: "request_oauth_authentication",
  SAVE_DRAFT_CONFIGURATION: "save_draft_configuration",
  SUBMIT_CONFIGURATION: "submit_configuration",
} as const;

export type ToolName = (typeof TOOL_NAMES)[keyof typeof TOOL_NAMES];
