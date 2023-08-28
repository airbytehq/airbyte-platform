module.exports = {
  rules: {
    "no-local-storage": require("./no-local-storage"),
    "no-hardcoded-connector-ids": require("./no-hardcoded-connector-ids"),
  },
  configs: {
    recommended: {
      rules: {
        "@airbyte/no-hardcoded-connector-ids": "error",
        "@airbyte/no-local-storage": "error",
      },
    },
  },
};
