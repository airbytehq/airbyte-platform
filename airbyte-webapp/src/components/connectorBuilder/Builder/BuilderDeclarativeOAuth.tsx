import { useEffect, useMemo } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { SchemaFormControl } from "components/forms/SchemaForm/Controls/SchemaFormControl";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { useConnectorBuilderResolve } from "core/services/connectorBuilder/ConnectorBuilderResolveContext";
import { useNotificationService } from "hooks/services/Notification";
import { OAUTH_REDIRECT_URL } from "hooks/services/useConnectorAuth";

import { AuthButtonBuilder } from "./AuthButtonBuilder";
import { useTestingValuesErrors } from "../StreamTestingPanel/TestingValuesMenu";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderWatch } from "../useBuilderWatch";
import { extractInterpolatedConfigPath } from "../utils";

export const ADVANCED_AUTH_PATH = "manifest.spec.advanced_auth";
export const OAUTH_INPUT_SPEC_PATH = `${ADVANCED_AUTH_PATH}.oauth_config_specification.oauth_connector_input_specification`;
const COMPLETE_OAUTH_OUTPUT_SPEC_PATH = `${ADVANCED_AUTH_PATH}.oauth_config_specification.complete_oauth_output_specification`;
const COMPLETE_OAUTH_SERVER_INPUT_SPEC_PATH = `${ADVANCED_AUTH_PATH}.oauth_config_specification.complete_oauth_server_input_specification`;
const COMPLETE_OAUTH_SERVER_OUTPUT_SPEC_PATH = `${ADVANCED_AUTH_PATH}.oauth_config_specification.complete_oauth_server_output_specification`;
const DEFAULT_ACCESS_TOKEN_KEY = "access_token";
const DEFAULT_REFRESH_TOKEN_KEY = "refresh_token";
export const OAUTH_BUTTON_NAME = "oauth-button";

export const BuilderDeclarativeOAuth = ({ authFieldPath }: { authFieldPath: (field: string) => string }) => {
  const { registerNotification } = useNotificationService();
  const { setValue, getValues } = useFormContext();
  const { projectId } = useConnectorBuilderResolve();

  const { validateAndTouch } = useBuilderErrors();

  const authenticatorAccessTokenKey =
    (useBuilderWatch(authFieldPath("access_token_name")) as string) || DEFAULT_ACCESS_TOKEN_KEY;
  const authenticatorRefreshTokenKey =
    (useBuilderWatch(authFieldPath("refresh_token_name")) as string) || DEFAULT_REFRESH_TOKEN_KEY;
  const authenticatorAccessTokenValue = useBuilderWatch(authFieldPath("access_token_value")) as string;
  const authenticatorRefreshTokenValue = useBuilderWatch(authFieldPath("refresh_token")) as string;
  const authenticatorScopes = useBuilderWatch(authFieldPath("scopes")) as string[] | undefined;
  const authenticatorClientId = useBuilderWatch(authFieldPath("client_id")) as string;
  const authenticatorClientSecret = useBuilderWatch(authFieldPath("client_secret")) as string;
  const isRefreshTokenUpdaterEnabled = !!useBuilderWatch(authFieldPath("refresh_token_updater")) as boolean;

  const interpolatedClientIdPath = useMemo(
    () => extractInterpolatedConfigPath(authenticatorClientId),
    [authenticatorClientId]
  );
  const interpolatedClientSecretPath = useMemo(
    () => extractInterpolatedConfigPath(authenticatorClientSecret),
    [authenticatorClientSecret]
  );

  const testingValuesErrors = useTestingValuesErrors();
  const hasNecessaryTestingValues = testingValuesErrors === 0;

  const oauthInputSpec = useBuilderWatch(OAUTH_INPUT_SPEC_PATH);
  const isOAuthInputSpecEnabled = useMemo(() => !!oauthInputSpec, [oauthInputSpec]);

  useEffect(() => {
    // if oauth input spec is disabled, automatically remove advanced_auth
    if (!isOAuthInputSpecEnabled) {
      setValue(ADVANCED_AUTH_PATH, undefined);
      return;
    }

    // oauth input spec is enabled, so automatically set the other fields in advanced_auth so that the user doesn't have to do it
    setValue(`${ADVANCED_AUTH_PATH}.auth_flow_type`, "oauth2.0");

    setValue(`${OAUTH_INPUT_SPEC_PATH}.scope`, authenticatorScopes?.join(" "));
    const extractOutputValue = isRefreshTokenUpdaterEnabled
      ? [authenticatorAccessTokenKey, authenticatorRefreshTokenKey]
      : [authenticatorAccessTokenKey];
    setValue(`${OAUTH_INPUT_SPEC_PATH}.extract_output`, extractOutputValue);

    const interpolatedAccessTokenConfigPath = extractInterpolatedConfigPath(authenticatorAccessTokenValue);
    const interpolatedRefreshTokenConfigPath = extractInterpolatedConfigPath(authenticatorRefreshTokenValue);
    if (interpolatedAccessTokenConfigPath || interpolatedRefreshTokenConfigPath) {
      const required: string[] = [];
      const properties: Record<string, unknown> = {};
      if (interpolatedAccessTokenConfigPath) {
        required.push(authenticatorAccessTokenKey);
        properties[authenticatorAccessTokenKey] = {
          type: "string",
          path_in_connector_config: interpolatedAccessTokenConfigPath.split("."),
        };
      }
      if (isRefreshTokenUpdaterEnabled && interpolatedRefreshTokenConfigPath) {
        required.push(authenticatorRefreshTokenKey);
        properties[authenticatorRefreshTokenKey] = {
          type: "string",
          path_in_connector_config: interpolatedRefreshTokenConfigPath.split("."),
        };
      }
      setValue(COMPLETE_OAUTH_OUTPUT_SPEC_PATH, {
        required,
        properties,
      });
    }

    setValue(COMPLETE_OAUTH_SERVER_INPUT_SPEC_PATH, {
      required: ["client_id", "client_secret"],
      properties: {
        client_id: {
          type: "string",
        },
        client_secret: {
          type: "string",
        },
      },
    });
    setValue(COMPLETE_OAUTH_SERVER_OUTPUT_SPEC_PATH, {
      required: ["client_id", "client_secret"],
      properties: {
        client_id: {
          type: "string",
          path_in_connector_config: interpolatedClientIdPath?.split("."),
        },
        client_secret: {
          type: "string",
          path_in_connector_config: interpolatedClientSecretPath?.split("."),
        },
      },
    });
  }, [
    authenticatorAccessTokenKey,
    authenticatorAccessTokenValue,
    authenticatorClientId,
    authenticatorClientSecret,
    authenticatorRefreshTokenKey,
    authenticatorRefreshTokenValue,
    authenticatorScopes,
    interpolatedClientIdPath,
    interpolatedClientSecretPath,
    isOAuthInputSpecEnabled,
    isRefreshTokenUpdaterEnabled,
    setValue,
  ]);

  return (
    <SchemaFormControl
      path={OAUTH_INPUT_SPEC_PATH}
      nonAdvancedFields={["consent_url", "access_token_url", "scope", "extract_output"]}
      overrideByPath={{
        [`${OAUTH_INPUT_SPEC_PATH}.scope`]: () => null,
        [`${OAUTH_INPUT_SPEC_PATH}.extract_output`]: () => null,
        [`${OAUTH_INPUT_SPEC_PATH}.consent_url`]: (path) => {
          return (
            <FlexContainer direction="column" gap="xl">
              <Message
                type="info"
                text={<FormattedMessage id="connectorForm.redirectUrl" values={{ url: OAUTH_REDIRECT_URL }} />}
              />
              <AuthButtonBuilder
                data-field-path={authFieldPath(OAUTH_BUTTON_NAME)}
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
                  const areRefreshTokensEnabled = !!getValues(authFieldPath("refresh_token_updater"));

                  if (!areRefreshTokensEnabled) {
                    if (authenticatorAccessTokenKey in payload) {
                      // update testing values with the returned access token
                      const accessTokenConfigKey = extractInterpolatedConfigPath(authenticatorAccessTokenValue);
                      setValue(`testingValues.${accessTokenConfigKey}`, payload[authenticatorAccessTokenKey], {
                        shouldValidate: true,
                        shouldDirty: true,
                        shouldTouch: true,
                      });
                    } else {
                      registerNotification({
                        id: "connectorBuilder.authentication.oauthButton.noAccessToken",
                        text: (
                          <FormattedMessage
                            id="connectorBuilder.authentication.oauthButton.noAccessToken"
                            values={{ accessTokenKey: authenticatorAccessTokenKey }}
                          />
                        ),
                        type: "error",
                      });
                    }
                  } else if (DEFAULT_REFRESH_TOKEN_KEY in payload) {
                    // update testing values with the returned refresh token
                    const refreshTokenConfigPath = extractInterpolatedConfigPath(authenticatorRefreshTokenValue);
                    setValue(`testingValues.${refreshTokenConfigPath}`, payload.refresh_token, {
                      shouldValidate: true,
                      shouldDirty: true,
                      shouldTouch: true,
                    });
                  } else {
                    registerNotification({
                      id: "connectorBuilder.authentication.oauthButton.noRefreshToken",
                      text: (
                        <FormattedMessage
                          id="connectorBuilder.authentication.oauthButton.noRefreshToken"
                          values={{ refreshTokenKey: DEFAULT_REFRESH_TOKEN_KEY }}
                        />
                      ),
                      type: "error",
                    });
                  }
                }}
              />
              <SchemaFormControl path={path} />
            </FlexContainer>
          );
        },
      }}
    />
  );
};
