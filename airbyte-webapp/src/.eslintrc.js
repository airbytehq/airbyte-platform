module.exports = {
  extends: ["plugin:jest/recommended"],
  rules: {
    "jest/consistent-test-it": ["warn", { fn: "it", withinDescribe: "it" }],
  },
};
