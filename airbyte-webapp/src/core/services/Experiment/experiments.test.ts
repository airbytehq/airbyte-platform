import { defaultExperimentValues } from "./experiments";

describe("defaultExperimentValues", () => {
  it("defaults settings.scimProvisioning to false so OSS builds and pre-LaunchDarkly renders fail closed on the ORGANIZATION_ADMIN-secured SCIM surfaces", () => {
    expect(defaultExperimentValues["settings.scimProvisioning"]).toBe(false);
  });
});
