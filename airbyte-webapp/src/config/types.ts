declare global {
  interface Window {
    TRACKING_STRATEGY?: string;
  }
}

export interface AirbyteWebappConfig {
  keycloakBaseUrl: string;
  segment: { token?: string; enabled: boolean };
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
  };
}
