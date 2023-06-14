import { ControlLabels } from "components/LabeledControl";

import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { links } from "utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderInputPlaceholder } from "./BuilderInputPlaceholder";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderOptional } from "./BuilderOptional";
import { KeyValueListField } from "./KeyValueListField";
import { RequestOptionFields } from "./RequestOptionFields";
import {
  API_KEY_AUTHENTICATOR,
  BASIC_AUTHENTICATOR,
  BEARER_AUTHENTICATOR,
  inferredAuthValues,
  OAUTH_AUTHENTICATOR,
  useBuilderWatch,
} from "../types";

export const AuthenticationSection: React.FC = () => {
  const analyticsService = useAnalyticsService();

  const grantType = useBuilderWatch("global.authenticator.grant_type");

  return (
    <BuilderCard docLink={links.connectorBuilderAuthentication} label={<ControlLabels label="Authentication" />}>
      <BuilderOneOf
        path="global.authenticator"
        label="Method"
        manifestPath="HttpRequester.properties.authenticator"
        manifestOptionPaths={[
          "ApiKeyAuthenticator",
          "BearerAuthenticator",
          "BasicHttpAuthenticator",
          "OAuthAuthenticator",
        ]}
        onSelect={(type) =>
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.AUTHENTICATION_METHOD_SELECT, {
            actionDescription: "Authentication method selected",
            auth_type: type,
          })
        }
        options={[
          { label: "No Auth", typeValue: "NoAuth", default: {} },
          {
            label: "API Key",
            typeValue: API_KEY_AUTHENTICATOR,
            default: {
              ...inferredAuthValues("ApiKeyAuthenticator"),
              inject_into: {
                type: "RequestOption",
                inject_into: "header",
                field_name: "",
              },
            },
            children: (
              <>
                <RequestOptionFields path="global.authenticator.inject_into" descriptor="token" excludePathInjection />
                <BuilderInputPlaceholder manifestPath="ApiKeyAuthenticator.properties.api_token" />
              </>
            ),
          },
          {
            label: "Bearer",
            typeValue: BEARER_AUTHENTICATOR,
            default: {
              ...inferredAuthValues("BearerAuthenticator"),
            },
            children: <BuilderInputPlaceholder manifestPath="BearerAuthenticator.properties.api_token" />,
          },
          {
            label: "Basic HTTP",
            typeValue: BASIC_AUTHENTICATOR,
            default: {
              ...inferredAuthValues("BasicHttpAuthenticator"),
            },
            children: (
              <>
                <BuilderInputPlaceholder manifestPath="BasicHttpAuthenticator.properties.username" />
                <BuilderInputPlaceholder manifestPath="BasicHttpAuthenticator.properties.password" />
              </>
            ),
          },
          {
            label: "OAuth",
            typeValue: OAUTH_AUTHENTICATOR,
            default: {
              ...inferredAuthValues("OAuthAuthenticator"),
              refresh_request_body: [],
              token_refresh_endpoint: "",
              grant_type: "refresh_token",
            },
            children: (
              <>
                <BuilderFieldWithInputs
                  type="string"
                  path="global.authenticator.token_refresh_endpoint"
                  manifestPath="OAuthAuthenticator.properties.token_refresh_endpoint"
                />
                <BuilderField
                  type="enum"
                  path="global.authenticator.grant_type"
                  options={["refresh_token", "client_credentials"]}
                  manifestPath="OAuthAuthenticator.properties.grant_type"
                />
                <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.client_id" />
                <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.client_secret" />
                {grantType === "refresh_token" && (
                  <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.refresh_token" />
                )}
                <BuilderOptional>
                  <BuilderField
                    type="array"
                    path="global.authenticator.scopes"
                    optional
                    manifestPath="OAuthAuthenticator.properties.scopes"
                  />
                  <BuilderFieldWithInputs
                    type="string"
                    path="global.authenticator.token_expiry_date_format"
                    optional
                    manifestPath="OAuthAuthenticator.properties.token_expiry_date_format"
                  />
                  <BuilderFieldWithInputs
                    type="string"
                    path="global.authenticator.expires_in_name"
                    optional
                    manifestPath="OAuthAuthenticator.properties.expires_in_name"
                  />
                  <BuilderFieldWithInputs
                    type="string"
                    path="global.authenticator.access_token_name"
                    optional
                    manifestPath="OAuthAuthenticator.properties.access_token_name"
                  />
                  <KeyValueListField
                    path="global.authenticator.refresh_request_body"
                    manifestPath="OAuthAuthenticator.properties.refresh_request_body"
                  />
                </BuilderOptional>
              </>
            ),
          },
        ]}
      />
    </BuilderCard>
  );
};
