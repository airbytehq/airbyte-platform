import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";

import {
  OAuthAuthenticatorRefreshTokenUpdater,
  SessionTokenAuthenticatorRequestAuthentication,
} from "core/api/types/ConnectorManifest";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderInputPlaceholder } from "./BuilderInputPlaceholder";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderOptional } from "./BuilderOptional";
import { ErrorHandlerSection } from "./ErrorHandlerSection";
import { InjectIntoFields } from "./InjectIntoFields";
import { KeyValueListField } from "./KeyValueListField";
import { getDescriptionByManifest, getLabelAndTooltip, getOptionsByManifest } from "./manifestHelpers";
import { RequestOptionSection } from "./RequestOptionSection";
import { ToggleGroupField } from "./ToggleGroupField";
import {
  API_KEY_AUTHENTICATOR,
  BASIC_AUTHENTICATOR,
  BEARER_AUTHENTICATOR,
  extractInterpolatedConfigKey,
  inferredAuthValues,
  OAUTH_ACCESS_TOKEN_INPUT,
  OAUTH_AUTHENTICATOR,
  OAUTH_TOKEN_EXPIRY_DATE_INPUT,
  SESSION_TOKEN_AUTHENTICATOR,
  useBuilderWatch,
  BuilderErrorHandler,
  LARGE_DURATION_OPTIONS,
  SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR,
  SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR,
  NO_AUTH,
  BuilderFormAuthenticator,
} from "../types";

export const AuthenticationSection: React.FC = () => {
  const analyticsService = useAnalyticsService();

  return (
    <BuilderCard docLink={links.connectorBuilderAuthentication} label="Authentication">
      <BuilderOneOf<BuilderFormAuthenticator>
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
          { label: "No Auth", default: { type: NO_AUTH } },
          {
            label: "API Key",
            default: {
              type: API_KEY_AUTHENTICATOR,
              ...inferredAuthValues("ApiKeyAuthenticator"),
              inject_into: {
                type: "RequestOption",
                inject_into: "header",
                field_name: "",
              },
            },
            children: (
              <>
                <InjectIntoFields path="global.authenticator.inject_into" descriptor="token" excludeValues={["path"]} />
                <BuilderInputPlaceholder manifestPath="ApiKeyAuthenticator.properties.api_token" />
              </>
            ),
          },
          {
            label: "Bearer",
            default: {
              type: BEARER_AUTHENTICATOR,
              ...(inferredAuthValues("BearerAuthenticator") as Record<"api_token", string>),
            },
            children: <BuilderInputPlaceholder manifestPath="BearerAuthenticator.properties.api_token" />,
          },
          {
            label: "Basic HTTP",
            default: {
              type: BASIC_AUTHENTICATOR,
              ...(inferredAuthValues("BasicHttpAuthenticator") as Record<"username" | "password", string>),
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
            default: {
              type: OAUTH_AUTHENTICATOR,
              ...(inferredAuthValues("OAuthAuthenticator") as Record<
                "client_id" | "client_secret" | "refresh_token" | "oauth_access_token" | "oauth_token_expiry_date",
                string
              >),
              refresh_request_body: [],
              token_refresh_endpoint: "",
              grant_type: "refresh_token",
            },
            children: <OAuthForm />,
          },
          {
            label: "Session Token",
            default: {
              type: SESSION_TOKEN_AUTHENTICATOR,
              login_requester: {
                url: "",
                authenticator: {
                  type: NO_AUTH,
                },
                httpMethod: "POST",
                requestOptions: {
                  requestParameters: [],
                  requestHeaders: [],
                  requestBody: {
                    type: "json_list",
                    values: [],
                  },
                },
              },
              session_token_path: [],
              expiration_duration: "",
              request_authentication: {
                type: SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR,
                inject_into: {
                  type: "RequestOption",
                  inject_into: "header",
                  field_name: "",
                },
              },
            },
            children: <SessionTokenForm />,
          },
        ]}
      />
    </BuilderCard>
  );
};

const OAuthForm = () => {
  const grantType = useBuilderWatch("global.authenticator.grant_type");
  const refreshToken = useBuilderWatch("global.authenticator.refresh_token");
  return (
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
        <>
          <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.refresh_token" />
          <ToggleGroupField<OAuthAuthenticatorRefreshTokenUpdater>
            label="Overwrite config with refresh token response"
            tooltip="If enabled, the refresh token response will overwrite the current OAuth config. This is useful if requesting a new access token invalidates the old refresh token."
            fieldPath="global.authenticator.refresh_token_updater"
            initialValues={{
              refresh_token_name: "",
              access_token_config_path: [OAUTH_ACCESS_TOKEN_INPUT],
              refresh_token_config_path: [extractInterpolatedConfigKey(refreshToken) || ""],
              token_expiry_date_config_path: [OAUTH_TOKEN_EXPIRY_DATE_INPUT],
            }}
          >
            <BuilderField
              type="string"
              path="global.authenticator.refresh_token_updater.refresh_token_name"
              optional
              manifestPath="OAuthAuthenticator.properties.refresh_token_updater.properties.refresh_token_name"
            />
          </ToggleGroupField>
        </>
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
  );
};

const SessionTokenForm = () => {
  const { label: loginRequesterLabel, tooltip: loginRequesterTooltip } = getLabelAndTooltip(
    "Session Token Retrieval",
    undefined,
    "SessionTokenAuthenticator.properties.login_requester",
    "global.authenticator.login_requester",
    true
  );
  return (
    <>
      <GroupControls label={<ControlLabels label={loginRequesterLabel} infoTooltipContent={loginRequesterTooltip} />}>
        <BuilderFieldWithInputs
          type="string"
          path="global.authenticator.login_requester.url"
          label="URL"
          tooltip="The full URL of where to send the request to retrieve the session token"
        />
        <BuilderField
          type="enum"
          path="global.authenticator.login_requester.httpMethod"
          options={getOptionsByManifest("HttpRequester.properties.http_method.anyOf.1")}
          manifestPath="HttpRequester.properties.http_method"
        />
        <BuilderOneOf<BuilderFormAuthenticator>
          path="global.authenticator.login_requester.authenticator"
          label="Authentication Method"
          manifestPath="HttpRequester.properties.authenticator"
          manifestOptionPaths={["ApiKeyAuthenticator", "BearerAuthenticator", "BasicHttpAuthenticator"]}
          options={[
            { label: "No Auth", default: { type: NO_AUTH } },
            {
              label: "API Key",
              default: {
                type: API_KEY_AUTHENTICATOR,
                ...inferredAuthValues("ApiKeyAuthenticator"),
                inject_into: {
                  type: "RequestOption",
                  inject_into: "header",
                  field_name: "",
                },
              },
              children: (
                <>
                  <InjectIntoFields
                    path="global.authenticator.login_requester.authenticator.inject_into"
                    descriptor="token"
                    excludeValues={["path"]}
                  />
                  <BuilderInputPlaceholder manifestPath="ApiKeyAuthenticator.properties.api_token" />
                </>
              ),
            },
            {
              label: "Bearer",
              default: {
                type: BEARER_AUTHENTICATOR,
                ...(inferredAuthValues(BEARER_AUTHENTICATOR) as Record<"api_token", string>),
              },
              children: <BuilderInputPlaceholder manifestPath="BearerAuthenticator.properties.api_token" />,
            },
            {
              label: "Basic HTTP",
              default: {
                type: BASIC_AUTHENTICATOR,
                ...(inferredAuthValues(BASIC_AUTHENTICATOR) as Record<"username" | "password", string>),
              },
              children: (
                <>
                  <BuilderInputPlaceholder manifestPath="BasicHttpAuthenticator.properties.username" />
                  <BuilderInputPlaceholder manifestPath="BasicHttpAuthenticator.properties.password" />
                </>
              ),
            },
          ]}
        />
        <RequestOptionSection
          inline
          basePath="global.authenticator.login_requester.requestOptions"
          omitInterpolationContext
        />
        <ToggleGroupField<BuilderErrorHandler[]>
          label="Error Handler"
          tooltip={getDescriptionByManifest("DefaultErrorHandler")}
          fieldPath="global.authenticator.login_requester.errorHandler"
          initialValues={[{ type: "DefaultErrorHandler" }]}
        >
          <ErrorHandlerSection inline basePath="global.authenticator.login_requester.errorHandler" />
        </ToggleGroupField>
      </GroupControls>
      <BuilderField
        type="array"
        path="global.authenticator.session_token_path"
        label="Session Token Path"
        tooltip="The path to the session token in the response body returned from the Session Token Retrieval request"
        directionalStyle
      />
      <BuilderField
        type="combobox"
        path="global.authenticator.expiration_duration"
        options={LARGE_DURATION_OPTIONS}
        manifestPath="SessionTokenAuthenticator.properties.expiration_duration"
        optional
      />
      <BuilderOneOf<SessionTokenAuthenticatorRequestAuthentication>
        path="global.authenticator.request_authentication"
        manifestPath="SessionTokenAuthenticator.properties.request_authentication"
        manifestOptionPaths={["SessionTokenRequestApiKeyAuthenticator", "SessionTokenRequestBearerAuthenticator"]}
        options={[
          {
            label: "API Key",
            default: {
              type: SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR,
              inject_into: {
                type: "RequestOption",
                inject_into: "header",
                field_name: "",
              },
            },
            children: (
              <InjectIntoFields
                path="global.authenticator.request_authentication.inject_into"
                descriptor="session token"
                label="Inject Session Token into outgoing HTTP Request"
                tooltip="Configure how the session token will be sent in requests to the source API"
                excludeValues={["path", "body_data", "body_json"]}
              />
            ),
          },
          {
            label: "Bearer",
            default: { type: SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR },
          },
        ]}
      />
    </>
  );
};
