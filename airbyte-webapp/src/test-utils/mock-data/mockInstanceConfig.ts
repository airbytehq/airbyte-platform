import { InstanceConfigurationResponse } from "core/request/AirbyteClient";

export const mockProInstanceConfig: InstanceConfigurationResponse = {
  auth: {
    clientId: "test-client-id",
    defaultRealm: "test-default-realm",
  },
  webappUrl: "http://test-airbyte-webapp-url.com",
  edition: "pro",
  licenseType: "pro",
};
