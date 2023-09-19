/* eslint-disable @typescript-eslint/no-var-requires */
const path = require("path");
const legacyFiles = require("./.eslintLegacyFolderStructure.js");

module.exports = {
  root: true,
  extends: [
    "react-app",
    "plugin:@typescript-eslint/recommended",
    "prettier",
    "plugin:prettier/recommended",
    "plugin:css-modules/recommended",
    "plugin:jsx-a11y/recommended",
    "plugin:@airbyte/recommended",
  ],
  plugins: ["@typescript-eslint", "prettier", "unused-imports", "css-modules", "jsx-a11y", "@airbyte", "check-file"],
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
  },
  rules: {
    "jsx-a11y/label-has-associated-control": "error",
    curly: "warn",
    "css-modules/no-undef-class": "off",
    "css-modules/no-unused-class": ["error", { camelCase: true }],
    "dot-location": "warn",
    "dot-notation": "warn",
    eqeqeq: "error",
    "prettier/prettier": "warn",
    "unused-imports/no-unused-imports": "warn",
    "no-else-return": "warn",
    "no-lonely-if": "warn",
    "no-inner-declarations": "off",
    "no-unused-vars": "off",
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
            pattern: "+(area|config|core|hooks|locales|packages|pages|services|types|views){/**,}",
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
    "@typescript-eslint/no-unused-vars": "warn",
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
  },
  parser: "@typescript-eslint/parser",
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
                except: ["index.ts", "cloud.ts", "types/", "errors/index.ts"],
                message:
                  "Only import from `core/api`, `core/api/cloud`, `core/api/errors`, or `core/api/types/*`. See also `core/api/README.md`.",
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
          }
        ]
      }
    },
    {
      // Only applies to files in src. Rules should be in here that are requiring type information
      // and thus require the below parserOptions.
      files: ["src/**/*"],
      excludedFiles: ["src/.eslintrc.js"],
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
      files: ["**/*.test.*", "**/*.stories.tsx"],
      rules: {
        "react/display-name": "off",
      },
    },
  ],
};
