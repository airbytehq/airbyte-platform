import { AirbyteWebappConfig } from "./types";

export const config: AirbyteWebappConfig = {
  keycloakBaseUrl: process.env.REACT_APP_KEYCLOAK_BASE_URL || window.location.origin,
  hockeyStackApiKey: process.env.REACT_APP_HOCKEYSTACK_API_KEY,
  segmentToken: process.env.REACT_APP_SEGMENT_TOKEN,
  apiUrl: process.env.REACT_APP_API_URL ?? "/api",
  connectorBuilderApiUrl: process.env.REACT_APP_CONNECTOR_BUILDER_API_URL ?? "",
  version: process.env.REACT_APP_VERSION,
  zendeskKey: process.env.REACT_APP_ZENDESK_KEY,
  launchDarkly: process.env.REACT_APP_LAUNCHDARKLY_KEY,
  datadog: {
    applicationId: process.env.REACT_APP_DATADOG_APPLICATION_ID,
    clientToken: process.env.REACT_APP_DATADOG_CLIENT_TOKEN,
    site: process.env.REACT_APP_DATADOG_SITE,
    service: process.env.REACT_APP_DATADOG_SERVICE,
    env: process.env.REACT_APP_DATADOG_ENV,
  },
};

export class MissingConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MissingConfigError";
  }
}
