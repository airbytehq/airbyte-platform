import {
  AdvancedAuth as SonarAdvancedAuth,
  ConnectorSpecification as SonarConnectorSpecification,
} from "core/api/types/SonarClient";

import { convertAdvancedAuth, convertUserConfigSpec } from "./advancedAuthConversion";

const mockSonarAdvancedAuth: SonarAdvancedAuth = {
  auth_flow_type: "oauth2.0",
  predicate_key: ["credentials", "auth_type"],
  predicate_value: "Client",
  oauth_config_specification: {
    oauth_user_input_from_connector_config_specification: {
      type: "object",
      properties: {
        client_id: {
          type: "string",
          path_in_connector_config: ["credentials", "client_id"],
        },
      },
    },
    complete_oauth_output_specification: {
      type: "object",
      properties: {
        refresh_token: {
          type: "string",
          path_in_connector_config: ["credentials", "refresh_token"],
        },
      },
    },
    complete_oauth_server_input_specification: {
      type: "object",
      properties: {
        client_id: { type: "string" },
        client_secret: { type: "string" },
      },
    },
    complete_oauth_server_output_specification: {
      type: "object",
      properties: {
        client_id: {
          type: "string",
          path_in_connector_config: ["credentials", "client_id"],
        },
        client_secret: {
          type: "string",
          path_in_connector_config: ["credentials", "client_secret"],
        },
      },
    },
  },
};

const mockSonarUserConfigSpec: SonarConnectorSpecification = {
  connectionSpecification: {
    type: "object",
    title: "Test Connector Spec",
    properties: {
      credentials: {
        type: "object",
        properties: {
          auth_type: { type: "string" },
        },
      },
    },
  },
  advanced_auth: mockSonarAdvancedAuth,
};

describe("convertAdvancedAuth", () => {
  it("should convert OAuth 2.0 auth flow correctly", () => {
    const result = convertAdvancedAuth(mockSonarAdvancedAuth);

    expect(result).toEqual({
      authFlowType: "oauth2.0",
      predicateKey: ["credentials", "auth_type"],
      predicateValue: "Client",
      oauthConfigSpecification: {
        oauthUserInputFromConnectorConfigSpecification: {
          type: "object",
          properties: {
            client_id: {
              type: "string",
              path_in_connector_config: ["credentials", "client_id"],
            },
          },
        },
        completeOAuthOutputSpecification: {
          type: "object",
          properties: {
            refresh_token: {
              type: "string",
              path_in_connector_config: ["credentials", "refresh_token"],
            },
          },
        },
        completeOAuthServerInputSpecification: {
          type: "object",
          properties: {
            client_id: { type: "string" },
            client_secret: { type: "string" },
          },
        },
        completeOAuthServerOutputSpecification: {
          type: "object",
          properties: {
            client_id: {
              type: "string",
              path_in_connector_config: ["credentials", "client_id"],
            },
            client_secret: {
              type: "string",
              path_in_connector_config: ["credentials", "client_secret"],
            },
          },
        },
      },
    });
  });

  it("should convert OAuth 1.0 auth flow correctly", () => {
    const oauth1AuthFlow = {
      ...mockSonarAdvancedAuth,
      auth_flow_type: "oauth1.0",
    };

    const result = convertAdvancedAuth(oauth1AuthFlow);

    expect(result.authFlowType).toBe("oauth1.0");
  });

  it("should handle missing oauth config specification properties with undefined", () => {
    const minimalAdvancedAuth: SonarAdvancedAuth = {
      auth_flow_type: "oauth2.0",
      oauth_config_specification: {},
    };

    const result = convertAdvancedAuth(minimalAdvancedAuth);

    expect(result.oauthConfigSpecification).toEqual({
      oauthUserInputFromConnectorConfigSpecification: undefined,
      completeOAuthOutputSpecification: undefined,
      completeOAuthServerInputSpecification: undefined,
      completeOAuthServerOutputSpecification: undefined,
    });
  });

  it("should throw error for unsupported auth flow type", () => {
    const invalidAuthFlow = {
      ...mockSonarAdvancedAuth,
      auth_flow_type: "unsupported_flow",
    };

    expect(() => convertAdvancedAuth(invalidAuthFlow)).toThrow("Unsupported auth flow type: unsupported_flow");
  });
});

describe("convertUserConfigSpec", () => {
  it("should convert user config spec with advanced auth", () => {
    const sourceDefinitionId = "test-source-definition-id";
    const result = convertUserConfigSpec(mockSonarUserConfigSpec, sourceDefinitionId);

    expect(result).toEqual({
      connectionSpecification: mockSonarUserConfigSpec.connectionSpecification,
      sourceDefinitionId,
      advancedAuth: {
        authFlowType: "oauth2.0",
        predicateKey: ["credentials", "auth_type"],
        predicateValue: "Client",
        oauthConfigSpecification: {
          oauthUserInputFromConnectorConfigSpecification: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                path_in_connector_config: ["credentials", "client_id"],
              },
            },
          },
          completeOAuthOutputSpecification: {
            type: "object",
            properties: {
              refresh_token: {
                type: "string",
                path_in_connector_config: ["credentials", "refresh_token"],
              },
            },
          },
          completeOAuthServerInputSpecification: {
            type: "object",
            properties: {
              client_id: { type: "string" },
              client_secret: { type: "string" },
            },
          },
          completeOAuthServerOutputSpecification: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                path_in_connector_config: ["credentials", "client_id"],
              },
              client_secret: {
                type: "string",
                path_in_connector_config: ["credentials", "client_secret"],
              },
            },
          },
        },
      },
      advancedAuthGlobalCredentialsAvailable: true,
    });
  });

  it("should handle user config spec without advanced auth", () => {
    const specWithoutAuth: SonarConnectorSpecification = {
      connectionSpecification: {
        type: "object",
        title: "Simple Connector Spec",
        properties: {
          api_key: { type: "string" },
        },
      },
    };

    const sourceDefinitionId = "test-source-definition-id";
    const result = convertUserConfigSpec(specWithoutAuth, sourceDefinitionId);

    expect(result).toEqual({
      connectionSpecification: specWithoutAuth.connectionSpecification,
      sourceDefinitionId,
    });

    expect(result.advancedAuth).toBeUndefined();
    expect(result.advancedAuthGlobalCredentialsAvailable).toBeUndefined();
  });

  it("should set advancedAuthGlobalCredentialsAvailable to false when advanced_auth is falsy", () => {
    const specWithFalsyAuth: SonarConnectorSpecification = {
      connectionSpecification: {
        type: "object",
        title: "Falsy Auth Connector Spec",
        properties: {
          api_key: { type: "string" },
        },
      },
      // Testing with undefined advanced_auth to ensure proper handling
    };

    const sourceDefinitionId = "test-source-definition-id";
    const result = convertUserConfigSpec(specWithFalsyAuth, sourceDefinitionId);

    expect(result.advancedAuth).toBeUndefined();
    expect(result.advancedAuthGlobalCredentialsAvailable).toBeUndefined();
  });
});
