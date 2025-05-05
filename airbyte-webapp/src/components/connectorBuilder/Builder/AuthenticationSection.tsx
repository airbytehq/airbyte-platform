import { ComponentProps, useCallback, useEffect, useRef } from "react";
import { useFormContext, useController } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { AssistButton } from "components/connectorBuilder/Builder/Assist/AssistButton";
import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";
import { Box } from "components/ui/Box";
import { Message } from "components/ui/Message";
import { Tooltip } from "components/ui/Tooltip";

import {
  HttpRequesterAuthenticator,
  SessionTokenAuthenticatorRequestAuthentication,
} from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";
import { useNotificationService } from "hooks/services/Notification";
import { OAUTH_REDIRECT_URL } from "hooks/services/useConnectorAuth";
import {
  useInitializedBuilderProject,
  useConnectorBuilderFormState,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder/ConnectorBuilderStateService";
import { AuthButtonBuilder } from "views/Connector/ConnectorForm/components/Sections/auth/AuthButton";

import styles from "./AuthenticationSection.module.scss";
import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderInputPlaceholder } from "./BuilderInputPlaceholder";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderOptional } from "./BuilderOptional";
import { BuilderRequestInjection } from "./BuilderRequestInjection";
import { ErrorHandlerSection } from "./ErrorHandlerSection";
import { KeyValueListField } from "./KeyValueListField";
import { getDescriptionByManifest, getLabelAndTooltip, getOptionsByManifest } from "./manifestHelpers";
import { RequestOptionSection } from "./RequestOptionSection";
import { ToggleGroupField } from "./ToggleGroupField";
import { manifestAuthenticatorToBuilder } from "../convertManifestToBuilderForm";
import { useTestingValuesErrors } from "../StreamTestingPanel/TestingValuesMenu";
import {
  API_KEY_AUTHENTICATOR,
  BASIC_AUTHENTICATOR,
  BEARER_AUTHENTICATOR,
  JWT_AUTHENTICATOR,
  OAUTH_AUTHENTICATOR,
  DeclarativeOAuthAuthenticatorType,
  SESSION_TOKEN_AUTHENTICATOR,
  BuilderErrorHandler,
  LARGE_DURATION_OPTIONS,
  SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR,
  SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR,
  NO_AUTH,
  BuilderFormAuthenticator,
  interpolateConfigKey,
  BuilderFormOAuthAuthenticator,
  builderAuthenticatorToManifest,
  builderInputsToSpec,
  BUILDER_SESSION_TOKEN_AUTH_DECODER_TYPES,
  extractInterpolatedConfigKey,
} from "../types";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderWatch, useBuilderWatchWithPreview } from "../useBuilderWatch";
import {
  LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE,
  getAuthKeyToDesiredLockedInput,
  useGetUniqueKey,
} from "../useLockedInputs";

export type AuthPath =
  | "formValues.global.authenticator"
  | `formValues.streams.${number}.creationRequester.authenticator`
  | `formValues.streams.${number}.pollingRequester.authenticator`
  | `formValues.streams.${number}.downloadRequester.authenticator`
  | `formValues.generatedStreams.${string}.${number}.creationRequester.authenticator`
  | `formValues.generatedStreams.${string}.${number}.pollingRequester.authenticator`
  | `formValues.generatedStreams.${string}.${number}.downloadRequester.authenticator`;

type AuthFieldPathFn = <T extends string>(fieldPath: T) => `${AuthPath}.${T}`;

interface AuthenticationSectionProps {
  authPath: AuthPath;
}

export const AuthenticationSection: React.FC<AuthenticationSectionProps> = ({ authPath }) => {
  const authFieldPath: AuthFieldPathFn = useCallback(
    <T extends string>(fieldPath: T) => `${authPath}.${fieldPath}` as const,
    [authPath]
  );
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();
  const getUniqueKey = useGetUniqueKey();
  const inputs = useBuilderWatch("formValues.inputs");

  const manifestAuthToBuilder = useCallback(
    (authenticator: HttpRequesterAuthenticator | undefined) =>
      manifestAuthenticatorToBuilder(authenticator, undefined, builderInputsToSpec(inputs)),
    [inputs]
  );

  const options: ComponentProps<typeof BuilderOneOf<BuilderFormAuthenticator>>["options"] = [
    { label: formatMessage({ id: "connectorBuilder.authentication.method.noAuth" }), default: { type: NO_AUTH } },
    {
      label: formatMessage({ id: "connectorBuilder.authentication.method.apiKey" }),
      default: {
        type: API_KEY_AUTHENTICATOR,
        inject_into: {
          type: "RequestOption",
          inject_into: "header",
          field_name: "",
        },
        api_token: interpolateConfigKey(
          getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[API_KEY_AUTHENTICATOR].api_token.key)
        ),
      },
      children: (
        <>
          <BuilderRequestInjection
            path={authFieldPath("inject_into")}
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
        api_token: interpolateConfigKey(
          getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[BEARER_AUTHENTICATOR].api_token.key)
        ),
      },
      children: <BuilderInputPlaceholder manifestPath="BearerAuthenticator.properties.api_token" />,
    },
    {
      label: formatMessage({ id: "connectorBuilder.authentication.method.basicHttp" }),
      default: {
        type: BASIC_AUTHENTICATOR,
        username: interpolateConfigKey(
          getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[BASIC_AUTHENTICATOR].username.key)
        ),
        password: interpolateConfigKey(
          getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[BASIC_AUTHENTICATOR].password.key)
        ),
      },
      children: (
        <>
          <BuilderInputPlaceholder manifestPath="BasicHttpAuthenticator.properties.username" />
          <BuilderInputPlaceholder manifestPath="BasicHttpAuthenticator.properties.password" />
        </>
      ),
    },
    {
      label: formatMessage({ id: "connectorBuilder.authentication.method.jwt" }),
      default: {
        type: JWT_AUTHENTICATOR,
        secret_key: interpolateConfigKey(
          getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[JWT_AUTHENTICATOR].secret_key.key)
        ),
        algorithm: "HS256",
        base64_encode_secret_key: false,
      },
      children: <JwtAuthForm authFieldPath={authFieldPath} />,
    },
    ...useOauthOptions(authPath, authFieldPath),
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
        decoder: "JSON",
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
      children: <SessionTokenForm authFieldPath={authFieldPath} />,
    },
  ];

  return (
    <BuilderCard
      docLink={links.connectorBuilderAuthentication}
      label={formatMessage({ id: "connectorBuilder.authentication.label" })}
      labelAction={<AssistButton assistKey="auth" />}
      inputsConfig={{
        toggleable: false,
        path: authPath,
        defaultValue: {
          type: NO_AUTH,
        },
        yamlConfig: {
          builderToManifest: builderAuthenticatorToManifest,
          manifestToBuilder: manifestAuthToBuilder,
          getLockedInputKeys: (authenticator: BuilderFormAuthenticator) => {
            return Object.keys(getAuthKeyToDesiredLockedInput(authenticator));
          },
        },
      }}
    >
      <BuilderOneOf<BuilderFormAuthenticator>
        path={authPath}
        label={formatMessage({ id: "connectorBuilder.authentication.method.label" })}
        manifestPath="HttpRequester.properties.authenticator"
        manifestOptionPaths={[
          "ApiKeyAuthenticator",
          "BearerAuthenticator",
          "BasicHttpAuthenticator",
          "OAuthAuthenticator",
          "JwtAuthenticator",
        ]}
        onSelect={(newType) => {
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.AUTHENTICATION_METHOD_SELECT, {
            actionDescription: "Authentication method selected",
            auth_type: newType,
          });
        }}
        options={options}
      />
    </BuilderCard>
  );
};

const JwtAuthForm = ({ authFieldPath }: { authFieldPath: AuthFieldPathFn }) => {
  return (
    <>
      <BuilderInputPlaceholder label="Secret Key" manifestPath="JwtAuthenticator.properties.secret_key" />
      <BuilderField
        label="Encode Secret Key (Base64 encoding)"
        type="boolean"
        path={authFieldPath("base64_encode_secret_key")}
        manifestPath="JwtAuthenticator.properties.base64_encode_secret_key"
      />
      <BuilderField
        label="Aglorithm"
        type="enum"
        path={authFieldPath("algorithm")}
        options={getOptionsByManifest("JwtAuthenticator.properties.algorithm")}
        manifestPath="JwtAuthenticator.properties.algorithm"
      />
      <BuilderField
        type="string"
        optional
        path={authFieldPath("header_prefix")}
        manifestPath="JwtAuthenticator.properties.header_prefix"
      />
      <BuilderField
        type="number"
        optional
        path={authFieldPath("token_duration")}
        manifestPath="JwtAuthenticator.properties.token_duration"
      />
      <GroupControls
        label={<ControlLabels label="JWT Header" infoTooltipContent="JWT headers used when signing JSON web token." />}
      >
        <BuilderField
          label="kid"
          tooltip="The 'kid' (Key Identifier) Header Parameter is a key ID for the user's account."
          type="jinja"
          optional
          path={authFieldPath("jwt_headers.kid")}
          manifestPath="JwtAuthenticator.properties.jwt_headers.kid"
        />
        <BuilderField
          label="typ"
          tooltip="The 'typ' (type) Header Parameter is used by JWT applications to declare the media type of this complete JWT."
          type="string"
          optional
          path={authFieldPath("jwt_headers.typ")}
          manifestPath="JwtAuthenticator.properties.jwt_headers.typ"
        />
        <BuilderField
          label="cty"
          tooltip="The 'cty' (content type) Header Parameter is used to convey structural information about the JWT."
          type="string"
          optional
          path={authFieldPath("jwt_headers.cty")}
          manifestPath="JwtAuthenticator.properties.jwt_headers.cty"
        />
        <KeyValueListField
          label="Additional JWT Header Key-Value Pairs"
          tooltip="Add additional JWT header parameters as required."
          path={authFieldPath("additional_jwt_headers")}
          manifestPath="JwtAuthenticator.properties.additional_jwt_headers"
          optional
        />
      </GroupControls>
      <GroupControls
        label={<ControlLabels label="JWT Payload" infoTooltipContent="JWT Payload used when signing JSON web token." />}
      >
        <BuilderField
          label="iss"
          tooltip="The 'iss' (Issuer) payload field is the user/principal that issued the JWT. Commonly a value unique to the user."
          type="jinja"
          optional
          path={authFieldPath("jwt_payload.iss")}
          manifestPath="JwtAuthenticator.properties.jwt_payload.iss"
        />
        <BuilderField
          label="sub"
          tooltip="The 'sub' (Subject) payload field of the JWT. Commonly defined by the API."
          type="string"
          optional
          path={authFieldPath("jwt_payload.sub")}
          manifestPath="JwtAuthenticator.properties.jwt_payload.sub"
        />
        <BuilderField
          label="aud"
          tooltip="The 'aud' (Audience) payload field recipient that the JWT is intended for. Commonly defined by the API."
          type="string"
          optional
          path={authFieldPath("jwt_payload.aud")}
          manifestPath="JwtAuthenticator.properties.jwt_payload.aud"
        />
        <KeyValueListField
          label="Additional JWT Payload Key-Value Pairs"
          tooltip="Add additional JWT payload fields as required."
          path={authFieldPath("additional_jwt_payload")}
          manifestPath="JwtAuthenticator.properties.additional_jwt_payload"
          optional
        />
      </GroupControls>
    </>
  );
};

const OAuthForm = ({ authFieldPath }: { authFieldPath: AuthFieldPathFn }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const { fieldValue: grantType } = useBuilderWatchWithPreview(authFieldPath("grant_type"));
  const getUniqueKey = useGetUniqueKey();

  return (
    <>
      <BuilderField
        type="jinja"
        path={authFieldPath("token_refresh_endpoint")}
        manifestPath="OAuthAuthenticator.properties.token_refresh_endpoint"
      />
      <BuilderField
        type="enum"
        path={authFieldPath("grant_type")}
        options={["refresh_token", "client_credentials"]}
        manifestPath="OAuthAuthenticator.properties.grant_type"
        onChange={(newValue) => {
          if (newValue === "client_credentials") {
            setValue(authFieldPath("refresh_token"), undefined);
          } else if (newValue === "refresh_token") {
            setValue(
              authFieldPath("refresh_token"),
              interpolateConfigKey(
                getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[OAUTH_AUTHENTICATOR].refresh_token.key)
              )
            );
          }
        }}
      />
      <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.client_id" />
      <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.client_secret" />
      {grantType === "refresh_token" && (
        <>
          <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.refresh_token" />
          <ToggleGroupField<BuilderFormOAuthAuthenticator["refresh_token_updater"]>
            label={formatMessage({ id: "connectorBuilder.authentication.refreshTokenUpdater.label" })}
            tooltip={formatMessage({ id: "connectorBuilder.authentication.refreshTokenUpdater.tooltip" })}
            fieldPath={authFieldPath("refresh_token_updater")}
            initialValues={{
              refresh_token_name: "",
              access_token: interpolateConfigKey(
                getUniqueKey(
                  LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[OAUTH_AUTHENTICATOR].refresh_token_updater
                    .access_token_config_path.key
                )
              ),
              token_expiry_date: interpolateConfigKey(
                getUniqueKey(
                  LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[OAUTH_AUTHENTICATOR].refresh_token_updater
                    .token_expiry_date_config_path.key
                )
              ),
            }}
          >
            <BuilderField
              type="jinja"
              path={authFieldPath("refresh_token_updater.refresh_token_name")}
              optional
              manifestPath="OAuthAuthenticator.properties.refresh_token_updater.properties.refresh_token_name"
            />
          </ToggleGroupField>
        </>
      )}
      <BuilderOptional>
        <BuilderField
          type="array"
          path={authFieldPath("scopes")}
          optional
          manifestPath="OAuthAuthenticator.properties.scopes"
        />
        <BuilderField
          type="jinja"
          path={authFieldPath("token_expiry_date_format")}
          optional
          manifestPath="OAuthAuthenticator.properties.token_expiry_date_format"
        />
        <BuilderField
          type="jinja"
          path={authFieldPath("expires_in_name")}
          optional
          manifestPath="OAuthAuthenticator.properties.expires_in_name"
        />
        <BuilderField
          type="jinja"
          path={authFieldPath("access_token_name")}
          optional
          manifestPath="OAuthAuthenticator.properties.access_token_name"
        />
        <KeyValueListField
          path={authFieldPath("refresh_request_body")}
          manifestPath="OAuthAuthenticator.properties.refresh_request_body"
        />
      </BuilderOptional>
    </>
  );
};

const payloadHasField = <FieldKey extends string>(
  payload: Record<string, unknown>,
  fieldKey: FieldKey
): payload is Record<FieldKey, string> => {
  return fieldKey in payload;
};

const DeclarativeOAuthForm = ({ authFieldPath }: { authFieldPath: AuthFieldPathFn }) => {
  const { projectId } = useInitializedBuilderProject();
  const { setValue, getValues } = useFormContext();
  const testingValuesErrors = useTestingValuesErrors();
  const { savingState } = useConnectorBuilderFormState();

  const canPerformOauthFlow = savingState === "saved";

  const { field: authenticatorScopesField } = useController({ name: authFieldPath("scopes") });
  const { field: authenticatorAccessTokenNameField } = useController({ name: authFieldPath("access_token_name") });
  const authenticatorAccessTokenValueField = useBuilderWatch(authFieldPath("access_token_value")) as string;
  const authenticatorRefreshTokenValueField = useBuilderWatch(authFieldPath("refresh_token")) as string;

  const { formatMessage } = useIntl();
  const getUniqueKey = useGetUniqueKey();

  const { registerNotification } = useNotificationService();
  const hasNecessaryTestingValues = testingValuesErrors === 0;

  const { handleScrollToField } = useConnectorBuilderFormManagementState();
  const { validateAndTouch } = useBuilderErrors();

  const authButtonBuilderRef = useRef(null);
  useEffect(() => {
    // Call handler in here to make sure it handles new scrollToField value from the context
    handleScrollToField(authButtonBuilderRef, "formValues.global.authenticator.declarative_oauth_flow");
  }, [handleScrollToField]);

  return (
    <>
      <Tooltip
        disabled={canPerformOauthFlow}
        control={
          <AuthButtonBuilder
            ref={authButtonBuilderRef}
            disabled={!canPerformOauthFlow}
            builderProjectId={projectId}
            onClick={
              hasNecessaryTestingValues
                ? undefined
                : () => {
                    registerNotification({
                      id: "connectorBuilder.authentication.oauthButton.inputsRequired",
                      text: <FormattedMessage id="connectorBuilder.authentication.oauthButton.inputsRequired" />,
                      type: "info",
                    });
                    setValue("view", { type: "inputs" });
                    validateAndTouch(undefined, [{ type: "inputs" }]);
                  }
            }
            onComplete={async (payload) => {
              const testingValues = getValues("testingValues"); // ensure we have the latest testing values
              const areRefreshTokensEnabled = !!getValues(authFieldPath("refresh_token_updater"));

              if (!areRefreshTokensEnabled) {
                const accessTokenKey = getValues(authFieldPath("declarative.access_token_key"));
                if (payloadHasField(payload, accessTokenKey)) {
                  // update testing values with the returned access token
                  const accessTokenConfigKey = extractInterpolatedConfigKey(authenticatorAccessTokenValueField);
                  setValue(
                    "testingValues",
                    {
                      ...testingValues,
                      [accessTokenConfigKey]: payload[accessTokenKey],
                    },
                    {
                      shouldValidate: true,
                      shouldDirty: true,
                      shouldTouch: true,
                    }
                  );
                } else {
                  registerNotification({
                    id: "connectorBuilder.authentication.oauthButton.noAccessToken",
                    text: (
                      <FormattedMessage
                        id="connectorBuilder.authentication.oauthButton.noAccessToken"
                        values={{ accessTokenKey }}
                      />
                    ),
                    type: "error",
                  });
                }
              } else if (payloadHasField(payload, "refresh_token")) {
                // update testing values with the returned refresh token
                const refreshTokenConfigKey = extractInterpolatedConfigKey(authenticatorRefreshTokenValueField);
                setValue(
                  "testingValues",
                  {
                    ...testingValues,
                    [refreshTokenConfigKey]: payload.refresh_token,
                  },
                  {
                    shouldValidate: true,
                    shouldDirty: true,
                    shouldTouch: true,
                  }
                );
              } else {
                registerNotification({
                  id: "connectorBuilder.authentication.oauthButton.noRefreshToken",
                  text: (
                    <FormattedMessage
                      id="connectorBuilder.authentication.oauthButton.noRefreshToken"
                      values={{ refreshTokenKey: "refresh_token" }}
                    />
                  ),
                  type: "error",
                });
              }
            }}
          />
        }
      >
        <FormattedMessage id="connectorBuilder.authentication.oauthButton.disabledTooltip" />
      </Tooltip>
      <Box mt="xl">
        <Message text={<FormattedMessage id="connectorForm.redirectUrl" values={{ url: OAUTH_REDIRECT_URL }} />} />
      </Box>
      <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.client_id" />
      <BuilderInputPlaceholder manifestPath="OAuthAuthenticator.properties.client_secret" />
      <BuilderField
        type="string"
        path={authFieldPath("declarative.consent_url")}
        manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.consent_url"
      />
      <BuilderField
        type="string"
        path={authFieldPath("declarative.access_token_url")}
        manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.access_token_url"
      />
      <BuilderField
        type="string"
        path={authFieldPath("declarative.scope")}
        manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.scope"
        optional
        onChange={(newValue) => {
          // also apply to authenticator's scopes
          authenticatorScopesField.onChange(newValue.split(/\s+/));
        }}
      />
      <BuilderField
        type="string"
        path={authFieldPath("declarative.access_token_key")}
        manifestPath="OAuthAuthenticator.properties.access_token_name"
        onChange={
          // also apply to authenticator's access token
          authenticatorAccessTokenNameField.onChange
        }
      />
      <ToggleGroupField<BuilderFormOAuthAuthenticator["refresh_token_updater"]>
        label={formatMessage({ id: "connectorBuilder.authentication.refreshTokenUpdater.label" })}
        tooltip={formatMessage({ id: "connectorBuilder.authentication.refreshTokenUpdater.tooltip" })}
        fieldPath={authFieldPath("refresh_token_updater")}
        initialValues={{
          refresh_token_name: "refresh_token",
          access_token: interpolateConfigKey(
            getUniqueKey(
              LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].refresh_token_updater
                .access_token_config_path.key
            )
          ),
          token_expiry_date: interpolateConfigKey(
            getUniqueKey(
              LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].refresh_token_updater
                .token_expiry_date_config_path.key
            )
          ),
        }}
      >
        <BuilderField
          type="jinja"
          path={authFieldPath("token_refresh_endpoint")}
          manifestPath="OAuthAuthenticator.properties.token_refresh_endpoint"
        />
        <BuilderField
          type="jinja"
          path={authFieldPath("refresh_token_updater.refresh_token_name")}
          optional
          manifestPath="OAuthAuthenticator.properties.refresh_token_updater.properties.refresh_token_name"
        />
      </ToggleGroupField>
      <BuilderOptional>
        <KeyValueListField
          path={authFieldPath("declarative.access_token_headers")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.access_token_headers"
        />
        <KeyValueListField
          path={authFieldPath("declarative.access_token_params")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.access_token_params"
        />
        <BuilderField
          type="string"
          path={authFieldPath("declarative.auth_code_key")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.auth_code_key"
        />
        <BuilderField
          type="string"
          path={authFieldPath("declarative.client_id_key")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.client_id_key"
        />
        <BuilderField
          type="string"
          path={authFieldPath("declarative.client_secret_key")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.client_secret_key"
        />
        <BuilderField
          type="string"
          path={authFieldPath("declarative.redirect_uri_key")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.redirect_uri_key"
        />
        <BuilderField
          type="string"
          path={authFieldPath("declarative.scope_key")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.scope_key"
        />
        <BuilderField
          type="string"
          path={authFieldPath("declarative.state_key")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.state_key"
        />
        <BuilderField
          type="jsoneditor"
          className={styles.stateField}
          path={authFieldPath("declarative.state")}
          optional
          manifestPath="OAuthConfigSpecification.properties.oauth_connector_input_specification.properties.state"
        />
      </BuilderOptional>
    </>
  );
};

const SessionTokenForm = ({ authFieldPath }: { authFieldPath: AuthFieldPathFn }) => {
  const { formatMessage } = useIntl();
  const { label: loginRequesterLabel, tooltip: loginRequesterTooltip } = getLabelAndTooltip(
    formatMessage({ id: "connectorBuilder.authentication.loginRequester.label" }),
    undefined,
    "SessionTokenAuthenticator.properties.login_requester",
    true
  );
  const getUniqueKey = useGetUniqueKey();

  return (
    <>
      <GroupControls label={<ControlLabels label={loginRequesterLabel} infoTooltipContent={loginRequesterTooltip} />}>
        <BuilderField
          type="jinja"
          path={authFieldPath("login_requester.url")}
          label={formatMessage({ id: "connectorBuilder.authentication.loginRequester.url.label" })}
          tooltip={formatMessage({ id: "connectorBuilder.authentication.loginRequester.url.tooltip" })}
        />
        <BuilderField
          type="enum"
          path={authFieldPath("login_requester.httpMethod")}
          options={getOptionsByManifest("HttpRequester.properties.http_method")}
          manifestPath="HttpRequester.properties.http_method"
        />
        <BuilderField
          type="enum"
          path={authFieldPath("decoder")}
          label={formatMessage({ id: "connectorBuilder.decoder.label" })}
          tooltip={formatMessage({ id: "connectorBuilder.decoder.tooltip" })}
          options={[...BUILDER_SESSION_TOKEN_AUTH_DECODER_TYPES]}
        />
        <BuilderOneOf<BuilderFormAuthenticator>
          path={authFieldPath("login_requester.authenticator")}
          label={formatMessage({ id: "connectorBuilder.authentication.loginRequester.authenticator.label" })}
          manifestPath="HttpRequester.properties.authenticator"
          manifestOptionPaths={[API_KEY_AUTHENTICATOR, BEARER_AUTHENTICATOR, BASIC_AUTHENTICATOR]}
          options={[
            {
              label: formatMessage({ id: "connectorBuilder.authentication.method.noAuth" }),
              default: { type: NO_AUTH },
            },
            {
              label: formatMessage({ id: "connectorBuilder.authentication.method.apiKey" }),
              default: {
                type: API_KEY_AUTHENTICATOR,
                inject_into: {
                  type: "RequestOption",
                  inject_into: "header",
                  field_name: "",
                },
                api_token: interpolateConfigKey(
                  getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[API_KEY_AUTHENTICATOR].api_token.key)
                ),
              },
              children: (
                <>
                  <BuilderRequestInjection
                    path={authFieldPath("login_requester.authenticator.inject_into")}
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
                api_token: interpolateConfigKey(
                  getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[BEARER_AUTHENTICATOR].api_token.key)
                ),
              },
              children: <BuilderInputPlaceholder manifestPath="BearerAuthenticator.properties.api_token" />,
            },
            {
              label: formatMessage({ id: "connectorBuilder.authentication.method.basicHttp" }),
              default: {
                type: BASIC_AUTHENTICATOR,
                username: interpolateConfigKey(
                  getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[BASIC_AUTHENTICATOR].username.key)
                ),
                password: interpolateConfigKey(
                  getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[BASIC_AUTHENTICATOR].password.key)
                ),
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
        <RequestOptionSection inline basePath={authFieldPath("login_requester.requestOptions")} />
        <ToggleGroupField<BuilderErrorHandler[]>
          label={formatMessage({ id: "connectorBuilder.authentication.loginRequester.errorHandler" })}
          tooltip={getDescriptionByManifest("DefaultErrorHandler")}
          fieldPath={authFieldPath("login_requester.errorHandler")}
          initialValues={[{ type: "DefaultErrorHandler" }]}
        >
          <ErrorHandlerSection inline basePath={authFieldPath("login_requester.errorHandler")} />
        </ToggleGroupField>
      </GroupControls>
      <BuilderField
        type="array"
        path={authFieldPath("session_token_path")}
        label={formatMessage({ id: "connectorBuilder.authentication.sessionTokenPath.label" })}
        tooltip={formatMessage({ id: "connectorBuilder.authentication.sessionTokenPath.tooltip" })}
        directionalStyle
      />
      <BuilderField
        type="combobox"
        path={authFieldPath("expiration_duration")}
        options={LARGE_DURATION_OPTIONS}
        manifestPath="SessionTokenAuthenticator.properties.expiration_duration"
        optional
      />
      <BuilderOneOf<SessionTokenAuthenticatorRequestAuthentication>
        path={authFieldPath("request_authentication")}
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
                path={authFieldPath("request_authentication.inject_into")}
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

const useOauthOptions = (authPath: AuthPath, authFieldPath: AuthFieldPathFn) => {
  const { formatMessage } = useIntl();
  const getUniqueKey = useGetUniqueKey();
  const isDeclarativeOAuthEnabled =
    useExperiment("connectorBuilder.declarativeOauth") && authPath === "formValues.global.authenticator";

  const baseOauthOption = {
    refresh_request_body: [],
    token_refresh_endpoint: "",
    grant_type: "refresh_token",
    client_id: interpolateConfigKey(
      getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[OAUTH_AUTHENTICATOR].client_id.key)
    ),
    client_secret: interpolateConfigKey(
      getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[OAUTH_AUTHENTICATOR].client_secret.key)
    ),
    refresh_token: interpolateConfigKey(
      getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[OAUTH_AUTHENTICATOR].refresh_token.key)
    ),
  };

  return !isDeclarativeOAuthEnabled
    ? [
        {
          label: formatMessage({ id: "connectorBuilder.authentication.method.oAuth" }),
          children: <OAuthForm authFieldPath={authFieldPath} />,
          default: {
            type: OAUTH_AUTHENTICATOR,
            ...baseOauthOption,
          },
        },
      ]
    : [
        {
          label: formatMessage({ id: "connectorBuilder.authentication.method.oAuth.declarative" }),
          children: <DeclarativeOAuthForm authFieldPath={authFieldPath} />,
          default: {
            type: DeclarativeOAuthAuthenticatorType,
            ...baseOauthOption,
            client_id: interpolateConfigKey(
              getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].client_id.key)
            ),
            client_secret: interpolateConfigKey(
              getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].client_secret.key)
            ),
            access_token_value: interpolateConfigKey(
              getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].access_token.key)
            ),
            refresh_token: interpolateConfigKey(
              getUniqueKey(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].refresh_token.key)
            ),
            declarative: {
              consent_url: "",
              access_token_url: "",
              scope: "",
              access_token_key: "access_token",
            },
          },
        },
        {
          label: formatMessage({ id: "connectorBuilder.authentication.method.oAuth.legacy" }),
          children: <OAuthForm authFieldPath={authFieldPath} />,
          default: {
            type: OAUTH_AUTHENTICATOR,
            ...baseOauthOption,
          },
        },
      ];
};
