import { defineConfig } from "cypress";

import baseConfig from "./cypress.cloud-config";

// TODO if we find that the set of config values we use changes with any frequency (well,
// any frequency above 0), use a deep merge utility library: manually merging and
// overriding config values property-by-property is verbose and error-prone
export default defineConfig({
  ...baseConfig,
  e2e: {
    ...baseConfig.e2e,
    baseUrl: "https://stage-cloud.airbyte.com",
    env: {
      ...baseConfig.e2e?.env,
      AIRBYTE_TEST_WORKSPACE_ID: "c26f21ca-8a03-41f4-bafb-8699ebc549a8",
    },
  },
});
