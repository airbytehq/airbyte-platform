/** @type {import('jest').Config} */
export default {
  rootDir: "../",

  // Buscar tests solo en custom-tests
  testMatch: ["<rootDir>/custom-tests/**/*.test.[jt]s?(x)"],
  testEnvironment: "jsdom",


  collectCoverage: true,

  // Recoger cobertura no de los tests
  collectCoverageFrom: [
    "<rootDir>/src/**/*.{js,jsx,ts,tsx}",
    "!<rootDir>/src/**/*.d.ts",
    "!<rootDir>/src/**/__tests__/**",
    "!<rootDir>/src/**/index.{ts,tsx,js,jsx}",
  ],

  coverageDirectory: "<rootDir>/coverage-custom",
  coverageReporters: ["text", "lcov", "html"],

  transform: {
    "^.+\\.[jt]sx?$": "babel-jest",
  },

  moduleFileExtensions: ["js", "jsx", "ts", "tsx", "json"],

  moduleNameMapper: {
    "\\.(css|scss|sass)$": "identity-obj-proxy",
    "\\.(jpg|jpeg|png|gif|eot|otf|webp|ttf|woff|woff2)$": "<rootDir>/custom-tests/__mocks__/fileMock.js",
    "\\.svg(\\?react)?$": "<rootDir>/custom-tests/__mocks__/svgrMock.js",

    "^components/(.*)$": "<rootDir>/src/components/$1",
    "^core/(.*)$": "<rootDir>/src/core/$1",
    "^hooks/(.*)$": "<rootDir>/src/hooks/$1",
    "^views/(.*)$": "<rootDir>/src/views/$1",
  },

  testPathIgnorePatterns: ["/node_modules/", "/dist/", "/build/"],

  verbose: true,
  moduleDirectories: ["node_modules", "src"],

  transformIgnorePatterns: [
    "node_modules/(?!(@airbytehq|uuid|react-intl|lodash-es)/)",
  ],
};
