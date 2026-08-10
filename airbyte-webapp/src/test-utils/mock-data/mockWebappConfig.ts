import { WebappConfigResponse } from "core/api/types/AirbyteClient";

export const mockWebappConfig: WebappConfigResponse = {
  version: "dev",
  edition: "community",
  datadogApplicationId: "test-datadog-app-id",
  datadogClientToken: "test-datadog-token",
  datadogSite: "test-datadog-site",
  datadogService: "test-datadog-service",
  datadogEnv: "test",
  keycloakBaseUrl: "test-keycloak-url",
  launchdarklyKey: "test-launchdarkly-key",
  segmentToken: "test-segment-token",
  zendeskKey: "test-zendesk-key",
  fullstoryGuidesOrgId: "test-fullstory-guides-org-id",
};
