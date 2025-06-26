import { datadogRum } from "@datadog/browser-rum";

import { WebappConfigResponse } from "core/api/types/AirbyteClient";

import { fullStorySessionLink } from "./fullstory";

export const loadDatadog = (config: WebappConfigResponse): void => {
  const { version, datadogApplicationId, datadogClientToken, datadogSite, datadogService, datadogEnv } = config;

  if (!datadogApplicationId || !datadogClientToken) {
    return;
  }

  datadogRum.init({
    applicationId: datadogApplicationId,
    clientToken: datadogClientToken,
    site: datadogSite,
    service: datadogService,
    version,
    env: datadogEnv,
    sampleRate: 100,
    sessionReplaySampleRate: 0,
    trackInteractions: false,
    trackResources: true,
    trackLongTasks: true,
    defaultPrivacyLevel: "mask-user-input",
  });

  datadogRum.startSessionReplayRecording();
};

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
  /**
   * Zendesk chat was tried to open while Zendesk didn't load properly.
   */
  ZENDESK_OPEN_FAILURE = "ZENDESK_OPEN_FAILURE",
}

export const trackAction = (action: AppActionCodes, context?: Record<string, unknown>) => {
  const contextWithFullStory = {
    ...context,
    fullStory: fullStorySessionLink(),
  };

  if (!datadogRum.getInternalContext()) {
    console.debug(`trackAction(${action}) failed because RUM is not initialized. Context: \n`, contextWithFullStory);
    return;
  }

  datadogRum.addAction(action, contextWithFullStory);
};

export const trackError = (error: Error, context?: Record<string, unknown>) => {
  const contextWithFullStory = {
    ...context,
    fullStory: fullStorySessionLink(),
  };

  if (!datadogRum.getInternalContext()) {
    console.debug(
      `trackError() failed because RUM is not initialized. Error: \n`,
      error,
      `Context: \n`,
      contextWithFullStory
    );
    return;
  }

  datadogRum.addError(error, contextWithFullStory);
};

type TimingName =
  | "DestinationSettingsPage"
  | "SourceSettingsPage"
  | "StreamStatusPage"
  | "CreditUsage" // Proxy for when the billing page has loaded
  | "WorkspacesList" // Proxy for when the workspaces page has loaded
  | "ConnectionsTable"; // Proxy for when the connections page has loaded

export const trackTiming = (name: TimingName) => {
  if (!datadogRum.getInternalContext()) {
    console.debug(`trackTiming(${name}) failed because RUM is not initialized.`);
    return;
  }

  datadogRum.addTiming(name);
};
