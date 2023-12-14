import { InstanceConfigurationResponse } from "core/api/types/AirbyteClient";

export const mockProInstanceConfig: InstanceConfigurationResponse = {
  auth: {
    clientId: "test-client-id",
    defaultRealm: "test-default-realm",
  },
  webappUrl: "http://test-airbyte-webapp-url.com",
  edition: "pro",
  licenseType: "pro",
  initialSetupComplete: true,
  defaultUserId: "00000000-0000-0000-0000-000000000000",
  defaultOrganizationId: "00000000-0000-0000-0000-000000000000",
  defaultWorkspaceId: "00000000-0000-0000-0000-000000000000",
};
