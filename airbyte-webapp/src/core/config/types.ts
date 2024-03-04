export interface AirbyteWebappConfig {
  keycloakBaseUrl: string;
  segmentToken?: string;
  apiUrl: string;
  connectorBuilderApiUrl: string;
  version?: string;
  cloudApiUrl: string;
  cloudPublicApiUrl?: string;
  firebase: {
    apiKey?: string;
    authDomain?: string;
    authEmulatorHost?: string;
  };
  zendeskKey?: string;
  launchDarkly?: string;
  datadog: {
    applicationId?: string;
    clientToken?: string;
    site?: string;
    service?: string;
    env?: string;
  };
}
