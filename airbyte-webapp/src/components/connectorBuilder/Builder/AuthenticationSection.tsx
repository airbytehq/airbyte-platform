import { useIntl } from "react-intl";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";

import {
  OAuthAuthenticatorRefreshTokenUpdater,
  SessionTokenAuthenticatorRequestAuthentication,
} from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderInputPlaceholder } from "./BuilderInputPlaceholder";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderOptional } from "./BuilderOptional";
import { BuilderRequestInjection } from "./BuilderRequestInjection";
import { ErrorHandlerSection } from "./ErrorHandlerSection";
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

const AUTH_PATH = "formValues.global.authenticator";
const authPath = <T extends string>(path: T) => `${AUTH_PATH}.${path}` as const;

export const AuthenticationSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();

  return (
    <BuilderCard
      docLink={links.connectorBuilderAuthentication}
      label={formatMessage({ id: "connectorBuilder.authentication.label" })}
    >
      <BuilderOneOf<BuilderFormAuthenticator>
        path={AUTH_PATH}
        label={formatMessage({ id: "connectorBuilder.authentication.method.label" })}
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
          { label: formatMessage({ id: "connectorBuilder.authentication.method.noAuth" }), default: { type: NO_AUTH } },
          {
            label: formatMessage({ id: "connectorBuilder.authentication.method.apiKey" }),
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
                <BuilderRequestInjection
                  path={authPath("inject_into")}
                  descriptor={formatMessage({ id: "connectorBuilder.authentication.injectInto.token" })}
                  excludeValues={["path"]}
                />
                <BuilderInputPlaceholder manifestPath="ApiKeyAuthenticator.properties.api_token" />
              </>
            ),
          },
          {
            label: formatMessage({ id: "connectorBuilder.authentication.method.bearer" }),
            default: {
              type: BEARER_AUTHENTICATOR,
              ...(inferredAuthValues("BearerAuthenticator") as Record<"api_token", string>),
            },
            children: <BuilderInputPlaceholder manifestPath="BearerAuthenticator.properties.api_token" />,
          },
          {
            label: formatMessage({ id: "connectorBuilder.authentication.method.basicHttp" }),
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
            label: formatMessage({ id: "connectorBuilder.authentication.method.oAuth" }),
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
            label: formatMessage({ id: "connectorBuilder.authentication.method.sessionToken" }),
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
  const { formatMessage } = useIntl();
  const grantType = useBuilderWatch(authPath("grant_type"));
  const refreshToken = useBuilderWatch(authPath("refresh_token"));
  return (
    <>
      <BuilderFieldWithInputs
        type="string"
        path={authPath("token_refresh_endpoint")}
        manifestPath="OAuthAuthenticator.properties.token_refresh_endpoint"
      />
      <BuilderField
        type="enum"
        path={authPath("grant_type")}
        options={["refresh_token", "client_credentials"]}
        manifestPath="OAuthAuthenticator.properties.grant_type"
      />
      <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.client_id" />
      <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.client_secret" />
      {grantType === "refresh_token" && (
        <>
          <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.refresh_token" />
          <ToggleGroupField<OAuthAuthenticatorRefreshTokenUpdater>
            label={formatMessage({ id: "connectorBuilder.authentication.refreshTokenUpdater.label" })}
            tooltip={formatMessage({ id: "connectorBuilder.authentication.refreshTokenUpdater.tooltip" })}
            fieldPath={authPath("refresh_token_updater")}
            initialValues={{
              refresh_token_name: "",
              access_token_config_path: [OAUTH_ACCESS_TOKEN_INPUT],
              refresh_token_config_path: [extractInterpolatedConfigKey(refreshToken) || ""],
              token_expiry_date_config_path: [OAUTH_TOKEN_EXPIRY_DATE_INPUT],
            }}
          >
            <BuilderField
              type="string"
              path={authPath("refresh_token_updater.refresh_token_name")}
              optional
              manifestPath="OAuthAuthenticator.properties.refresh_token_updater.properties.refresh_token_name"
            />
          </ToggleGroupField>
        </>
      )}
      <BuilderOptional>
        <BuilderField
          type="array"
          path={authPath("scopes")}
          optional
          manifestPath="OAuthAuthenticator.properties.scopes"
        />
        <BuilderFieldWithInputs
          type="string"
          path={authPath("token_expiry_date_format")}
          optional
          manifestPath="OAuthAuthenticator.properties.token_expiry_date_format"
        />
        <BuilderFieldWithInputs
          type="string"
          path={authPath("expires_in_name")}
          optional
          manifestPath="OAuthAuthenticator.properties.expires_in_name"
        />
        <BuilderFieldWithInputs
          type="string"
          path={authPath("access_token_name")}
          optional
          manifestPath="OAuthAuthenticator.properties.access_token_name"
        />
        <KeyValueListField
          path={authPath("refresh_request_body")}
          manifestPath="OAuthAuthenticator.properties.refresh_request_body"
        />
      </BuilderOptional>
    </>
  );
};

const SessionTokenForm = () => {
  const { formatMessage } = useIntl();
  const { label: loginRequesterLabel, tooltip: loginRequesterTooltip } = getLabelAndTooltip(
    formatMessage({ id: "connectorBuilder.authentication.loginRequester.label" }),
    undefined,
    "SessionTokenAuthenticator.properties.login_requester",
    authPath("login_requester"),
    true
  );
  return (
    <>
      <GroupControls label={<ControlLabels label={loginRequesterLabel} infoTooltipContent={loginRequesterTooltip} />}>
        <BuilderFieldWithInputs
          type="string"
          path={authPath("login_requester.url")}
          label={formatMessage({ id: "connectorBuilder.authentication.loginRequester.url.label" })}
          tooltip={formatMessage({ id: "connectorBuilder.authentication.loginRequester.url.tooltip" })}
        />
        <BuilderField
          type="enum"
          path={authPath("login_requester.httpMethod")}
          options={getOptionsByManifest("HttpRequester.properties.http_method.anyOf.1")}
          manifestPath="HttpRequester.properties.http_method"
        />
        <BuilderOneOf<BuilderFormAuthenticator>
          path={authPath("login_requester.authenticator")}
          label={formatMessage({ id: "connectorBuilder.authentication.loginRequester.authenticator.label" })}
          manifestPath="HttpRequester.properties.authenticator"
          manifestOptionPaths={["ApiKeyAuthenticator", "BearerAuthenticator", "BasicHttpAuthenticator"]}
          options={[
            {
              label: formatMessage({ id: "connectorBuilder.authentication.method.noAuth" }),
              default: { type: NO_AUTH },
            },
            {
              label: formatMessage({ id: "connectorBuilder.authentication.method.apiKey" }),
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
                  <BuilderRequestInjection
                    path={authPath("login_requester.authenticator.inject_into")}
                    descriptor={formatMessage({ id: "connectorBuilder.authentication.injectInto.token" })}
                    excludeValues={["path"]}
                  />
                  <BuilderInputPlaceholder manifestPath="ApiKeyAuthenticator.properties.api_token" />
                </>
              ),
            },
            {
              label: formatMessage({ id: "connectorBuilder.authentication.method.bearer" }),
              default: {
                type: BEARER_AUTHENTICATOR,
                ...(inferredAuthValues(BEARER_AUTHENTICATOR) as Record<"api_token", string>),
              },
              children: <BuilderInputPlaceholder manifestPath="BearerAuthenticator.properties.api_token" />,
            },
            {
              label: formatMessage({ id: "connectorBuilder.authentication.method.basicHttp" }),
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
        <RequestOptionSection inline basePath={authPath("login_requester.requestOptions")} omitInterpolationContext />
        <ToggleGroupField<BuilderErrorHandler[]>
          label={formatMessage({ id: "connectorBuilder.authentication.loginRequester.errorHandler" })}
          tooltip={getDescriptionByManifest("DefaultErrorHandler")}
          fieldPath={authPath("login_requester.errorHandler")}
          initialValues={[{ type: "DefaultErrorHandler" }]}
        >
          <ErrorHandlerSection inline basePath={authPath("login_requester.errorHandler")} />
        </ToggleGroupField>
      </GroupControls>
      <BuilderField
        type="array"
        path={authPath("session_token_path")}
        label={formatMessage({ id: "connectorBuilder.authentication.sessionTokenPath.label" })}
        tooltip={formatMessage({ id: "connectorBuilder.authentication.sessionTokenPath.tooltip" })}
        directionalStyle
      />
      <BuilderField
        type="combobox"
        path={authPath("expiration_duration")}
        options={LARGE_DURATION_OPTIONS}
        manifestPath="SessionTokenAuthenticator.properties.expiration_duration"
        optional
      />
      <BuilderOneOf<SessionTokenAuthenticatorRequestAuthentication>
        path={authPath("request_authentication")}
        manifestPath="SessionTokenAuthenticator.properties.request_authentication"
        manifestOptionPaths={["SessionTokenRequestApiKeyAuthenticator", "SessionTokenRequestBearerAuthenticator"]}
        options={[
          {
            label: formatMessage({ id: "connectorBuilder.authentication.method.apiKey" }),
            default: {
              type: SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR,
              inject_into: {
                type: "RequestOption",
                inject_into: "header",
                field_name: "",
              },
            },
            children: (
              <BuilderRequestInjection
                path={authPath("request_authentication.inject_into")}
                descriptor={formatMessage({ id: "connectorBuilder.authentication.injectInto.sessionToken" })}
                label={formatMessage({ id: "connectorBuilder.authentication.requestAuthentication.injectInto.label" })}
                tooltip={formatMessage({
                  id: "connectorBuilder.authentication.requestAuthentication.injectInto.tooltip",
                })}
                excludeValues={["path", "body_data", "body_json"]}
              />
            ),
          },
          {
            label: formatMessage({ id: "connectorBuilder.authentication.method.bearer" }),
            default: { type: SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR },
          },
        ]}
      />
    </>
  );
};
