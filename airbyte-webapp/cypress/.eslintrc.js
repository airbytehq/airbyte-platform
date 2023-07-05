module.exports = {
  env: {
    browser: true,
    node: true,
  },
  extends: ["plugin:cypress/recommended"],
  rules: {
    "cypress/no-unnecessary-waiting": "warn",
    "no-template-curly-in-string": "off",
    "@typescript-eslint/no-unused-expressions": "off",
  },
};
