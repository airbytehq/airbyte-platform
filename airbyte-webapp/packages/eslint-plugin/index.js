module.exports = {
  rules: {
    "no-local-storage": require("./no-local-storage"),
    "no-hardcoded-connector-ids": require("./no-hardcoded-connector-ids"),
    "no-invalid-css-module-imports": require("./no-invalid-css-module-imports"),
  },
  configs: {
    recommended: {
      rules: {
        "@airbyte/no-hardcoded-connector-ids": "error",
        "@airbyte/no-local-storage": "error",
        "@airbyte/no-invalid-css-module-imports": "error",
      },
    },
  },
};
