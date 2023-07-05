/**
 * Action codes are used to log specific runtime events that we want to analyse in datadog.
 * This is useful for tracking when and how frequently certain code paths on the frontend are
 * encountered in production.
 */
export enum AppActionCodes {
  /**
   * LaunchDarkly did not load in time and was ignored
   */
  LD_LOAD_TIMEOUT = "LD_LOAD_TIMEOUT",
  UNEXPECTED_CONNECTION_FLOW_STATE = "UNEXPECTED_CONNECTION_FLOW_STATE",
  CONNECTOR_DEFINITION_NOT_FOUND = "CONNECTOR_DEFINITION_NOT_FOUND",
  CONNECTOR_DOCUMENTATION_FETCH_ERROR = "CONNECTOR_DOCUMENTATION_FETCH_ERROR",
  CONNECTOR_DOCUMENTATION_NOT_MARKDOWN = "CONNECTOR_DOCUMENTATION_NOT_MARKDOWN",
  /**
   * Zendesk chat was tried to open while Zendesk didn't load properly.
   */
  ZENDESK_OPEN_FAILURE = "ZENDESK_OPEN_FAILURE",
}
