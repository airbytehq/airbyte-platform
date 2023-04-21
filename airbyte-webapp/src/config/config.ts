import { AirbyteWebappConfig } from "./types";

export const config: AirbyteWebappConfig = {
  segment: {
    token: process.env.REACT_APP_SEGMENT_TOKEN,
    enabled: !window.TRACKING_STRATEGY || window.TRACKING_STRATEGY === "segment",
  },
  fathomSiteId: process.env.REACT_APP_FATHOM_SITE_ID,
  apiUrl: process.env.REACT_APP_API_URL ?? "/api",
  cloudApiUrl: process.env.REACT_APP_CLOUD_API_URL ?? "/cloud",
  connectorBuilderApiUrl: process.env.REACT_APP_CONNECTOR_BUILDER_API_URL ?? "/connector-builder-api",
  version: process.env.REACT_APP_VERSION,
  cloudPublicApiUrl: process.env.REACT_APP_CLOUD_PUBLIC_API_URL ?? "/cloud_api",
  firebase: {
    apiKey: process.env.REACT_APP_FIREBASE_API_KEY,
    authDomain: process.env.REACT_APP_FIREBASE_AUTH_DOMAIN,
    authEmulatorHost: process.env.REACT_APP_FIREBASE_AUTH_EMULATOR_HOST,
  },
  intercom: {
    appId: process.env.REACT_APP_INTERCOM_APP_ID,
  },
  launchDarkly: process.env.REACT_APP_LAUNCHDARKLY_KEY,
  datadog: {
    applicationId: process.env.REACT_APP_DATADOG_APPLICATION_ID,
    clientToken: process.env.REACT_APP_DATADOG_CLIENT_TOKEN,
    site: process.env.REACT_APP_DATADOG_SITE,
    service: process.env.REACT_APP_DATADOG_SERVICE,
  },
};

export class MissingConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MissingConfigError";
  }
}
