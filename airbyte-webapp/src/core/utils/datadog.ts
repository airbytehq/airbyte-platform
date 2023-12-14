import { datadogRum } from "@datadog/browser-rum";

import { config } from "core/config";

export const loadDatadog = (): void => {
  const {
    version,
    datadog: { applicationId, clientToken, site, service },
  } = config;

  if (!applicationId || !clientToken) {
    return;
  }

  datadogRum.init({
    applicationId,
    clientToken,
    site,
    service,
    version,
    sampleRate: 100,
    sessionReplaySampleRate: 0,
    trackInteractions: false,
    trackResources: true,
    trackLongTasks: true,
    defaultPrivacyLevel: "mask-user-input",
  });

  datadogRum.startSessionReplayRecording();
};

export const trackAction = (action: string, context?: Record<string, unknown>) => {
  if (!datadogRum.getInternalContext()) {
    console.debug(`trackAction(${action}) failed because RUM is not initialized. Context: \n`, context);
    return;
  }

  datadogRum.addAction(action, context);
};

export const trackError = (error: Error, context?: Record<string, unknown>) => {
  if (!datadogRum.getInternalContext()) {
    console.debug(`trackError() failed because RUM is not initialized. Error: \n`, error, `Context: \n`, context);
    return;
  }

  datadogRum.addError(error, context);
};
