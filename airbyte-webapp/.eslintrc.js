/* eslint-disable @typescript-eslint/no-var-requires */
const path = require("node:path");

const legacyFiles = require("./.eslintLegacyFolderStructure.js");

const confusingBrowserGlobals = [
  "addEventListener",
  "blur",
  "close",
  "closed",
  "confirm",
  "defaultStatus",
  "defaultstatus",
  "event",
  "external",
  "find",
  "focus",
  "frameElement",
  "frames",
  "history",
  "innerHeight",
  "innerWidth",
  "length",
  "location",
  "locationbar",
  "menubar",
  "moveBy",
  "moveTo",
  "name",
  "onblur",
  "onerror",
  "onfocus",
  "onload",
  "onresize",
  "onunload",
  "open",
  "opener",
  "opera",
  "outerHeight",
  "outerWidth",
  "pageXOffset",
  "pageYOffset",
  "parent",
  "print",
  "removeEventListener",
  "resizeBy",
  "resizeTo",
  "screen",
  "screenLeft",
  "screenTop",
  "screenX",
  "screenY",
  "scroll",
  "scrollbars",
  "scrollBy",
  "scrollTo",
  "scrollX",
  "scrollY",
  "self",
  "status",
  "statusbar",
  "stop",
  "toolbar",
  "top",
];

module.exports = {
  root: true,
  extends: [
    "plugin:@typescript-eslint/recommended",
    "prettier",
    "plugin:prettier/recommended",
    "plugin:css-modules/recommended",
    "plugin:jsx-a11y/recommended",
    "plugin:@airbyte/recommended",
  ],
  plugins: [
    "@typescript-eslint",
    "import",
    "unused-imports",
    "prettier",
    "css-modules",
    "jsx-a11y",
    "@airbyte",
    "check-file",
    "react",
    "react-hooks",
  ],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: 2020,
    sourceType: "module",
    ecmaFeatures: {
      jsx: true,
    },
  },
  settings: {
    "import/resolver": {
      typescript: {
        project: path.resolve(__dirname, "tsconfig.json"),
      },
    },
    react: {
      version: "detect",
    },
  },
  rules: {
    "jsx-a11y/label-has-associated-control": [
      "error",
      {
        depth: 3,
      },
    ],
    curly: "warn",
    "css-modules/no-undef-class": "off",
    "css-modules/no-unused-class": ["error", { camelCase: true }],
    "dot-location": ["warn", "property"],
    "dot-notation": "warn",
    eqeqeq: ["error", "smart"],
    "prettier/prettier": "warn",
    "unused-imports/no-unused-imports": "warn",
    "no-else-return": "warn",
    "no-lonely-if": "warn",
    "no-inner-declarations": "off",
    "no-useless-computed-key": "warn",
    "no-useless-return": "warn",
    "no-var": "warn",
    "object-shorthand": ["warn", "always"],
    "prefer-arrow-callback": "warn",
    "prefer-const": "warn",
    "prefer-destructuring": ["warn", { AssignmentExpression: { array: true } }],
    "prefer-object-spread": "warn",
    "prefer-template": "warn",
    "spaced-comment": ["warn", "always", { markers: ["/"] }],
    yoda: "warn",
    "import/no-duplicates": ["warn", { considerQueryString: true }],
    "import/order": [
      "warn",
      {
        "newlines-between": "always",
        groups: ["type", "builtin", "external", "internal", ["parent", "sibling"], "index"],
        pathGroupsExcludedImportTypes: ["builtin"],
        pathGroups: [
          {
            pattern: "components{/**,}",
            group: "internal",
          },
          {
            pattern: "+(area|core|hooks|locales|packages|pages|services|types|views){/**,}",
            group: "internal",
            position: "after",
          },
        ],
        alphabetize: {
          order: "asc" /* sort in ascending order. Options: ['ignore', 'asc', 'desc'] */,
          caseInsensitive: true /* ignore case. Options: [true, false] */,
        },
      },
    ],
    "@typescript-eslint/array-type": ["warn", { default: "array-simple" }],
    "@typescript-eslint/ban-ts-comment": [
      "warn",
      {
        "ts-expect-error": "allow-with-description",
      },
    ],
    "@typescript-eslint/ban-types": "warn",
    "@typescript-eslint/consistent-indexed-object-style": ["warn", "record"],
    "@typescript-eslint/consistent-type-definitions": ["warn", "interface"],
    "react/display-name": "warn",
    "react/function-component-definition": [
      "warn",
      {
        namedComponents: "arrow-function",
        unnamedComponents: "arrow-function",
      },
    ],
    "react/no-danger": "error",
    "react/jsx-boolean-value": "warn",
    "react/jsx-curly-brace-presence": "warn",
    "react/jsx-fragments": "warn",
    "react/jsx-no-useless-fragment": ["warn", { allowExpressions: true }],
    "react/self-closing-comp": "warn",
    "react/style-prop-object": ["warn", { allow: ["FormattedNumber"] }],
    "no-restricted-imports": [
      "error",
      {
        paths: [
          {
            name: "react-use",
            importNames: ["useLocalStorage"],
            message:
              'Please use our wrapped version of this hook with `import { useLocalStorage } from "core/utils/useLocalStorage";` instead.',
          },
          {
            name: "lodash",
            message: 'Please use `import [function] from "lodash/[function]";` instead.',
          },
          {
            name: "react-router-dom",
            importNames: ["Link"],
            message: 'Please use `import { Link, ExternalLink } from "components/ui/Link";` instead.',
          },
        ],
        patterns: ["!lodash/*"],
      },
    ],

    // Original eslint-config-react-app rules
    "react/jsx-uses-vars": "warn",
    "react/jsx-uses-react": "warn",
    "array-callback-return": "warn",
    "new-parens": "warn",
    "no-caller": "warn",
    "no-cond-assign": ["warn", "except-parens"],
    "no-const-assign": "warn",
    "no-control-regex": "warn",
    "no-delete-var": "warn",
    "no-dupe-args": "warn",
    "no-dupe-keys": "warn",
    "no-duplicate-case": "warn",
    "no-empty-character-class": "warn",
    "no-empty-pattern": "warn",
    "no-eval": "warn",
    "no-ex-assign": "warn",
    "no-extend-native": "warn",
    "no-extra-bind": "warn",
    "no-extra-label": "warn",
    "no-fallthrough": "warn",
    "no-func-assign": "warn",
    "no-implied-eval": "warn",
    "no-invalid-regexp": "warn",
    "no-iterator": "warn",
    "no-label-var": "warn",
    "no-labels": ["warn", { allowLoop: true, allowSwitch: false }],
    "no-lone-blocks": "warn",
    "no-loop-func": "warn",
    "no-mixed-operators": [
      "warn",
      {
        groups: [
          ["&", "|", "^", "~", "<<", ">>", ">>>"],
          ["==", "!=", "===", "!==", ">", ">=", "<", "<="],
          ["&&", "||"],
          ["in", "instanceof"],
        ],
        allowSamePrecedence: false,
      },
    ],
    "no-multi-str": "warn",
    "no-global-assign": "warn",
    "no-unsafe-negation": "warn",
    "no-new-func": "warn",
    "no-new-object": "warn",
    "no-new-symbol": "warn",
    "no-new-wrappers": "warn",
    "no-obj-calls": "warn",
    "no-octal": "warn",
    "no-octal-escape": "warn",
    "no-regex-spaces": "warn",
    "no-restricted-syntax": ["warn", "WithStatement"],
    "no-script-url": "warn",
    "no-self-assign": "warn",
    "no-self-compare": "warn",
    "no-sequences": "warn",
    "no-shadow-restricted-names": "warn",
    "no-sparse-arrays": "warn",
    "no-template-curly-in-string": "warn",
    "no-this-before-super": "warn",
    "no-throw-literal": "warn",
    "no-restricted-globals": ["error"].concat(confusingBrowserGlobals),
    "no-unreachable": "warn",
    "no-unused-labels": "warn",
    "no-useless-concat": "warn",
    "no-useless-constructor": "warn",
    "no-useless-escape": "warn",
    "no-useless-rename": [
      "warn",
      {
        ignoreDestructuring: false,
        ignoreImport: false,
        ignoreExport: false,
      },
    ],
    "no-with": "warn",
    "no-whitespace-before-property": "warn",
    "react-hooks/exhaustive-deps": "warn",
    "require-yield": "warn",
    "rest-spread-spacing": ["warn", "never"],
    strict: ["warn", "never"],
    "unicode-bom": ["warn", "never"],
    "use-isnan": "warn",
    "valid-typeof": "warn",
    "no-restricted-properties": [
      "error",
      {
        object: "require",
        property: "ensure",
        message:
          "Please use import() instead. More info: https://facebook.github.io/create-react-app/docs/code-splitting",
      },
      {
        object: "System",
        property: "import",
        message:
          "Please use import() instead. More info: https://facebook.github.io/create-react-app/docs/code-splitting",
      },
    ],
    "getter-return": "warn",

    // https://github.com/benmosher/eslint-plugin-import/tree/master/docs/rules
    "import/first": "error",
    "import/no-amd": "error",
    "import/no-anonymous-default-export": "warn",
    "import/no-webpack-loader-syntax": "error",

    // https://github.com/yannickcr/eslint-plugin-react/tree/master/docs/rules
    "react/forbid-foreign-prop-types": ["warn", { allowInPropTypes: true }],
    "react/jsx-no-comment-textnodes": "warn",
    "react/jsx-no-duplicate-props": "warn",
    "react/jsx-no-target-blank": "warn",
    "react/jsx-no-undef": "error",
    "react/jsx-pascal-case": [
      "warn",
      {
        allowAllCaps: true,
        ignore: [],
      },
    ],
    "react/no-danger-with-children": "warn",
    "react/no-deprecated": "warn",
    "react/no-direct-mutation-state": "warn",
    "react/no-is-mounted": "warn",
    "react/no-typos": "error",
    "react/require-render-return": "error",

    // https://github.com/evcohen/eslint-plugin-jsx-a11y/tree/master/docs/rules
    "jsx-a11y/alt-text": "warn",
    "jsx-a11y/anchor-has-content": "warn",
    "jsx-a11y/anchor-is-valid": [
      "warn",
      {
        aspects: ["noHref", "invalidHref"],
      },
    ],
    "jsx-a11y/aria-activedescendant-has-tabindex": "warn",
    "jsx-a11y/aria-props": "warn",
    "jsx-a11y/aria-proptypes": "warn",
    "jsx-a11y/aria-role": ["warn", { ignoreNonDOM: true }],
    "jsx-a11y/aria-unsupported-elements": "warn",
    "jsx-a11y/heading-has-content": "warn",
    "jsx-a11y/iframe-has-title": "warn",
    "jsx-a11y/img-redundant-alt": "warn",
    "jsx-a11y/no-access-key": "warn",
    "jsx-a11y/no-distracting-elements": "warn",
    "jsx-a11y/no-redundant-roles": "warn",
    "jsx-a11y/role-has-required-aria-props": "warn",
    "jsx-a11y/role-supports-aria-props": "warn",
    "jsx-a11y/scope": "warn",

    // https://github.com/facebook/react/tree/main/packages/eslint-plugin-react-hooks
    "react-hooks/rules-of-hooks": "error",
    // TypeScript's `noFallthroughCasesInSwitch` option is more robust (#6906)
    "default-case": "off",
    // 'tsc' already handles this (https://github.com/typescript-eslint/typescript-eslint/issues/291)
    "no-dupe-class-members": "off",
    // 'tsc' already handles this (https://github.com/typescript-eslint/typescript-eslint/issues/477)
    "no-undef": "off",

    // Add TypeScript specific rules (and turn off ESLint equivalents)
    "@typescript-eslint/consistent-type-assertions": "warn",
    "no-array-constructor": "off",
    "@typescript-eslint/no-array-constructor": "warn",
    "no-redeclare": "off",
    "@typescript-eslint/no-redeclare": "warn",
    "no-use-before-define": "off",
    "@typescript-eslint/no-use-before-define": [
      "warn",
      {
        functions: false,
        classes: false,
        variables: false,
        typedefs: false,
      },
    ],
    "no-unused-expressions": "off",
    "@typescript-eslint/no-unused-expressions": [
      "error",
      {
        allowShortCircuit: true,
        allowTernary: true,
        allowTaggedTemplates: true,
      },
    ],
    "no-unused-vars": "off",
    "@typescript-eslint/no-unused-vars": [
      "warn",
      {
        args: "none",
        ignoreRestSiblings: true,
      },
    ],
    "@typescript-eslint/no-useless-constructor": "warn",
  },
  overrides: [
    {
      // Forbid importing anything from within `core/api/`, except the explicit files that are meant to be accessed outside this folder.
      files: ["src/**/*"],
      excludedFiles: ["src/core/api/**"],
      rules: {
        "import/no-restricted-paths": [
          "error",
          {
            basePath: path.resolve(__dirname, "./src"),
            zones: [
              {
                target: ".",
                from: "./core/api",
                except: ["index.ts", "cloud.ts", "types/"],
                message:
                  "Only import from `core/api`, `core/api/cloud`, or `core/api/types/*`. See also `core/api/README.md`.",
              },
            ],
          },
        ],
      },
    },
    {
      files: ["scripts/**/*", "packages/**/*"],
      rules: {
        "@typescript-eslint/no-var-requires": "off",
      },
    },
    {
      // Prevent new files being created in the legacy folder structure.
      // This makes sure to forbid any new files in the legacy folders
      // we want to get rid of in favor of the new folder structure outlined
      // in the README.md.
      files: ["src/**/*"],
      // Only exclude the files that already existed and haven't been moved
      // to the new folder structure yet
      excludedFiles: legacyFiles,
      rules: {
        "check-file/filename-blocklist": [
          "error",
          {
            // Services should be in either src/core/services or src/area/*/services
            "src/services/**/*": "src/core/services/**/*",
            // Hooks (not belonging to any service) should just be in src/core/utils or src/area/*/utils
            "src/hooks/**/*": "src/core/utils/*",
            // Components should be in either ui/ (basic UI components) or src/area/*/components/*
            "src/views/**/*": "src/area/*/components/*",
          },
        ],
      },
    },
    {
      // Only applies to files in src. Rules should be in here that are requiring type information
      // and thus require the below parserOptions.
      files: ["src/**/*"],
      parserOptions: {
        tsconfigRootDir: __dirname,
        project: "./tsconfig.json",
      },
      rules: {
        "@typescript-eslint/await-thenable": "warn",
        "@typescript-eslint/no-unnecessary-type-assertion": "warn",
      },
    },
    {
      // Only apply to jest test files

      files: ["src/**/*.test.*"],
      extends: ["plugin:jest/recommended"],
      rules: {
        "jest/consistent-test-it": ["warn", { fn: "it", withinDescribe: "it" }],
      },
    },
    {
      // Only for cypress files
      files: ["cypress/**"],
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
    },
    {
      files: ["**/*.test.*", "**/*.stories.tsx"],
      rules: {
        "react/display-name": "off",
      },
    },
  ],
};
