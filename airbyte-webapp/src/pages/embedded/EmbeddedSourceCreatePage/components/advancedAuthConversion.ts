import { AdvancedAuth, AdvancedAuthAuthFlowType, SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import {
  AdvancedAuth as SonarAdvancedAuth,
  ConnectorSpecification as SonarConnectorSpecification,
} from "core/api/types/SonarClient";

const toAuthFlowType = (v: string): AdvancedAuthAuthFlowType => {
  if (v === "oauth1.0" || v === "oauth2.0") {
    return v;
  }
  throw new Error(`Unsupported auth flow type: ${v}`);
};

/**
 * Converts Sonar advanced auth format to Airbyte advanced auth format
 */
export const convertAdvancedAuth = (sonarAdvancedAuth: SonarAdvancedAuth): AdvancedAuth => {
  return {
    authFlowType: toAuthFlowType(sonarAdvancedAuth.auth_flow_type),
    predicateKey: sonarAdvancedAuth.predicate_key || undefined,
    predicateValue: sonarAdvancedAuth.predicate_value || undefined,
    oauthConfigSpecification: {
      oauthUserInputFromConnectorConfigSpecification:
        sonarAdvancedAuth.oauth_config_specification?.oauth_user_input_from_connector_config_specification || undefined,
      completeOAuthOutputSpecification:
        sonarAdvancedAuth.oauth_config_specification?.complete_oauth_output_specification || undefined,
      completeOAuthServerInputSpecification:
        sonarAdvancedAuth.oauth_config_specification?.complete_oauth_server_input_specification || undefined,
      completeOAuthServerOutputSpecification:
        sonarAdvancedAuth.oauth_config_specification?.complete_oauth_server_output_specification || undefined,
    },
  };
};

/**
 * Converts Sonar user config spec to Airbyte SourceDefinitionSpecification format
 */
export const convertUserConfigSpec = (
  sonarUserConfigSpec: SonarConnectorSpecification,
  sourceDefinitionId: string
): SourceDefinitionSpecification => {
  const { advanced_auth, ...restOfSpec } = sonarUserConfigSpec;

  return {
    ...restOfSpec,
    sourceDefinitionId,
    ...(advanced_auth && {
      advancedAuth: convertAdvancedAuth(advanced_auth),
      advancedAuthGlobalCredentialsAvailable: Boolean(advanced_auth),
    }),
  } as SourceDefinitionSpecification;
};
