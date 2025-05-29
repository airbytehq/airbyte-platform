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
    launchdarklyKey: process.env.REACT_APP_LAUNCHDARKLY_KEY ?? config.launchdarklyKey,
    osanoKey: process.env.REACT_APP_OSANO_KEY ?? config.osanoKey,
    segmentToken: process.env.REACT_APP_SEGMENT_TOKEN ?? config.segmentToken,
    zendeskKey: process.env.REACT_APP_ZENDESK_KEY ?? config.zendeskKey,
  };
}

export function useWebappConfig() {
  const config = useContext(WebappConfigContext);
  if (!config) {
    throw new Error("ConfigContext not found");
  }
  return config;
}
