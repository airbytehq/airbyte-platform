import { defineConfig } from "cypress";
import merge from "lodash/merge";

import baseConfig from "./cypress.cloud-config";

export default defineConfig(
  merge({}, baseConfig, {
    projectId: "8qdqa1",
    viewportHeight: 800,
    viewportWidth: 1280,
    e2e: {
      baseUrl: "https://stage-cloud.airbyte.com",
      env: {
        AIRBYTE_TEST_WORKSPACE_ID: "c26f21ca-8a03-41f4-bafb-8699ebc549a8",
        LOGIN_URL: null,
      },
    },
  })
);
