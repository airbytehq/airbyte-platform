import React, { useContext } from "react";

import { getWebappConfig } from "core/api";
import { WebappConfigResponse } from "core/api/types/AirbyteClient";

export const WebappConfigContext = React.createContext<WebappConfigResponse | null>(null);

export const WebappConfigContextProvider: React.FC<React.PropsWithChildren<{ config: WebappConfigResponse }>> = ({
  children,
  config,
}) => {
  return <WebappConfigContext.Provider value={config}>{children}</WebappConfigContext.Provider>;
};

export async function loadConfig() {
  const config = await getWebappConfig({ getAccessToken: () => Promise.resolve(null) });
  return {
    ...config,
    edition: config.edition.toLowerCase(),
    datadogApplicationId: config.datadogApplicationId || process.env.REACT_APP_DATADOG_APPLICATION_ID,
    datadogClientToken: config.datadogClientToken || process.env.REACT_APP_DATADOG_CLIENT_TOKEN,
    datadogSite: config.datadogSite || process.env.REACT_APP_DATADOG_SITE,
    datadogService: config.datadogService || process.env.REACT_APP_DATADOG_SERVICE,
    datadogEnv: config.datadogEnv || process.env.REACT_APP_DATADOG_ENV,
    hockeystackApiKey: config.hockeystackApiKey || process.env.REACT_APP_HOCKEYSTACK_API_KEY,
    launchdarklyKey: config.launchdarklyKey || process.env.REACT_APP_LAUNCHDARKLY_KEY,
    osanoKey: config.osanoKey || process.env.REACT_APP_OSANO_KEY,
    segmentToken: config.segmentToken || process.env.REACT_APP_SEGMENT_TOKEN,
    sonarApiUrl: config.sonarApiUrl || process.env.REACT_APP_SONAR_API_URL,
    zendeskKey: config.zendeskKey || process.env.REACT_APP_ZENDESK_KEY,
    posthogApiKey: config.posthogApiKey || process.env.REACT_APP_POSTHOG_API_KEY,
    posthogHost: config.posthogHost || process.env.REACT_APP_POSTHOG_HOST,
  };
}

export function useWebappConfig() {
  const config = useContext(WebappConfigContext);
  if (!config) {
    throw new Error("ConfigContext not found");
  }
  return config;
}
