import { useEffect } from "react";
import { useFormContext } from "react-hook-form";

import { useExperiment } from "hooks/services/Experiment";

import {
  API_KEY_AUTHENTICATOR,
  BASIC_AUTHENTICATOR,
  BEARER_AUTHENTICATOR,
  DeclarativeOAuthAuthenticatorType,
  BuilderFormAuthenticator,
  BuilderFormInput,
  BuilderFormValues,
  BuilderStream,
  NO_AUTH,
  OAUTH_AUTHENTICATOR,
  SESSION_TOKEN_AUTHENTICATOR,
  extractInterpolatedConfigKey,
  isYamlString,
  JWT_AUTHENTICATOR,
  YamlString,
} from "./types";
import { useBuilderWatch } from "./useBuilderWatch";

export const useUpdateLockedInputs = () => {
  const formValues = useBuilderWatch("formValues");
  const testingValues = useBuilderWatch("testingValues");
  const { setValue, trigger } = useFormContext();
  const isSchemaFormEnabled = useExperiment("connectorBuilder.schemaForm");

  useEffect(() => {
    if (isSchemaFormEnabled) {
      return;
    }

    const keyToDesiredLockedInput = getKeyToDesiredLockedInput(formValues.global.authenticator, formValues.streams);
    const existingLockedInputKeys = formValues.inputs.filter((input) => input.isLocked).map((input) => input.key);
    const lockedInputKeysToCreate = Object.keys(keyToDesiredLockedInput).filter(
      (key) => !existingLockedInputKeys.includes(key)
    );
    const lockedInputKeysToDelete = existingLockedInputKeys.filter((key) => !keyToDesiredLockedInput[key]);
    if (lockedInputKeysToCreate.length === 0 && lockedInputKeysToDelete.length === 0) {
      return;
    }

    const updatedInputs = formValues.inputs.filter((input) => !lockedInputKeysToDelete.includes(input.key));
    lockedInputKeysToCreate.forEach((key) => {
      updatedInputs.push({
        ...keyToDesiredLockedInput[key],
        key,
        isLocked: true,
      });
    });
    setValue("formValues.inputs", updatedInputs);

    // create a new testingValues object with each of the keys in lockedInputKeysToDelete removed
    const newTestingValues = { ...testingValues };
    lockedInputKeysToDelete.forEach((key) => {
      delete newTestingValues[key];
    });
    setValue("testingValues", newTestingValues);
    trigger("testingValues");
  }, [
    formValues.global.authenticator,
    formValues.inputs,
    formValues.streams,
    isSchemaFormEnabled,
    setValue,
    testingValues,
    trigger,
  ]);
};

export const useGetUniqueKey = () => {
  const builderInputs = useBuilderWatch("formValues.inputs");
  const builderStreams = useBuilderWatch("formValues.streams");

  // If reuseIncrementalField is set, find the first stream which has the corresponding incremental field
  // set to user input and return its key. Otherwise, return a unique version of the desired key.
  return (desiredKey: string, reuseIncrementalField?: "start_datetime" | "end_datetime") => {
    if (reuseIncrementalField) {
      let existingKey: string | undefined = undefined;
      builderStreams.some((stream) => {
        const incrementalSync =
          stream.requestType === "sync" ? stream.incrementalSync : stream.creationRequester.incrementalSync;
        if (incrementalSync && !isYamlString(incrementalSync)) {
          const incrementalDatetime = incrementalSync[reuseIncrementalField];
          if (incrementalDatetime.type === "user_input") {
            existingKey = extractInterpolatedConfigKey(incrementalDatetime.value);
            return true;
          }
        }
        return false;
      });
      if (existingKey) {
        return existingKey;
      }
    }

    const existingNonLockedKeys = builderInputs.filter((input) => !input.isLocked).map((input) => input.key);
    let key = desiredKey;
    let i = 2;
    while (existingNonLockedKeys.includes(key)) {
      key = `${desiredKey}_${i}`;
      i++;
    }
    return key;
  };
};

export function getKeyToDesiredLockedInput(
  authenticator: BuilderFormValues["global"]["authenticator"],
  streams: BuilderStream[]
): Record<string, BuilderFormInput> {
  const authKeyToDesiredInput = {
    ...getAuthKeyToDesiredLockedInput(authenticator),
    ...streams.reduce((acc, stream) => {
      if (stream.requestType === "async") {
        return {
          ...acc,
          ...getAuthKeyToDesiredLockedInput(stream.creationRequester.authenticator),
          ...getAuthKeyToDesiredLockedInput(stream.pollingRequester.authenticator),
          ...getAuthKeyToDesiredLockedInput(stream.downloadRequester.authenticator),
        };
      }
      return acc;
    }, {}),
  };

  const incrementalStartDateKeys = new Set<string>();
  const incrementalEndDateKeys = new Set<string>();
  streams.forEach((stream) => {
    const incrementalSync =
      stream.requestType === "sync" ? stream.incrementalSync : stream.creationRequester.incrementalSync;
    if (incrementalSync && !isYamlString(incrementalSync)) {
      const startDatetime = incrementalSync.start_datetime;
      if (startDatetime.type === "user_input") {
        incrementalStartDateKeys.add(extractInterpolatedConfigKey(startDatetime.value));
      }

      const endDatetime = incrementalSync.end_datetime;
      if (endDatetime.type === "user_input") {
        incrementalEndDateKeys.add(extractInterpolatedConfigKey(endDatetime.value));
      }
    }
  });

  const incrementalKeyToDesiredInput: Record<string, BuilderFormInput> = {
    ...Array.from(incrementalStartDateKeys).reduce(
      (acc, key) => ({
        ...acc,
        [key]: LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME.start_datetime,
      }),
      {}
    ),
    ...Array.from(incrementalEndDateKeys).reduce(
      (acc, key) => ({
        ...acc,
        [key]: LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME.end_datetime,
      }),
      {}
    ),
  };

  return {
    ...authKeyToDesiredInput,
    ...incrementalKeyToDesiredInput,
  };
}

export function getAuthKeyToDesiredLockedInput(
  authenticator: BuilderFormAuthenticator | YamlString
): Record<string, BuilderFormInput> {
  if (isYamlString(authenticator)) {
    return {};
  }

  switch (authenticator.type) {
    case API_KEY_AUTHENTICATOR:
    case BEARER_AUTHENTICATOR:
      const apiTokenKey = extractInterpolatedConfigKey(authenticator.api_token);
      return {
        ...(apiTokenKey && { [apiTokenKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[authenticator.type].api_token }),
      };

    case BASIC_AUTHENTICATOR:
      const usernameKey = extractInterpolatedConfigKey(authenticator.username);
      const passwordKey = extractInterpolatedConfigKey(authenticator.password);
      return {
        [usernameKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[BASIC_AUTHENTICATOR].username,
        ...(passwordKey && {
          [passwordKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[BASIC_AUTHENTICATOR].password,
        }),
      };

    case JWT_AUTHENTICATOR:
      const secretKey = extractInterpolatedConfigKey(authenticator.secret_key);

      return {
        ...(secretKey && {
          [secretKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[authenticator.type].secret_key,
        }),
      };

    case OAUTH_AUTHENTICATOR: {
      const clientIdKey = extractInterpolatedConfigKey(authenticator.client_id);
      const clientSecretKey = extractInterpolatedConfigKey(authenticator.client_secret);
      const refreshTokenKey = extractInterpolatedConfigKey(authenticator.refresh_token);
      const accessTokenKey = extractInterpolatedConfigKey(authenticator.refresh_token_updater?.access_token);
      const tokenExpiryDateKey = extractInterpolatedConfigKey(authenticator.refresh_token_updater?.token_expiry_date);

      return {
        ...(clientIdKey && {
          [clientIdKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[authenticator.type].client_id,
        }),
        ...(clientSecretKey && {
          [clientSecretKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[authenticator.type].client_secret,
        }),
        ...(refreshTokenKey && {
          [refreshTokenKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[authenticator.type].refresh_token,
        }),
        ...(accessTokenKey && {
          [accessTokenKey]:
            LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[authenticator.type].refresh_token_updater.access_token_config_path,
        }),
        ...(tokenExpiryDateKey && {
          [tokenExpiryDateKey]:
            LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[authenticator.type].refresh_token_updater
              .token_expiry_date_config_path,
        }),
      };
    }

    case DeclarativeOAuthAuthenticatorType: {
      const isRefreshTokenFlowEnabled = !!authenticator.refresh_token_updater;

      const clientIdKey = extractInterpolatedConfigKey(authenticator.client_id);
      const clientSecretKey = extractInterpolatedConfigKey(authenticator.client_secret);
      const refreshTokenKey = extractInterpolatedConfigKey(authenticator.refresh_token);
      const accessTokenKey = extractInterpolatedConfigKey(authenticator.access_token_value);

      return {
        ...(clientIdKey && {
          [clientIdKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].client_id,
        }),
        ...(clientSecretKey && {
          [clientSecretKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].client_secret,
        }),
        ...(isRefreshTokenFlowEnabled && refreshTokenKey
          ? {
              [refreshTokenKey]:
                LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].refresh_token,
            }
          : {}),
        ...(!isRefreshTokenFlowEnabled && accessTokenKey
          ? {
              [accessTokenKey]: LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[DeclarativeOAuthAuthenticatorType].access_token,
            }
          : {}),
      };
    }

    case SESSION_TOKEN_AUTHENTICATOR:
      const loginRequesterAuthenticator = authenticator.login_requester.authenticator;
      return loginRequesterAuthenticator ? getAuthKeyToDesiredLockedInput(loginRequesterAuthenticator) : {};

    default:
      return {};
  }
}

export const LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE = {
  [NO_AUTH]: {},
  [API_KEY_AUTHENTICATOR]: {
    api_token: {
      key: "api_key",
      required: true,
      definition: {
        type: "string" as const,
        title: "API Key",
        airbyte_secret: true,
      },
    },
  },
  [BEARER_AUTHENTICATOR]: {
    api_token: {
      key: "api_key",
      required: true,
      definition: {
        type: "string" as const,
        title: "API Key",
        airbyte_secret: true,
      },
    },
  },
  [BASIC_AUTHENTICATOR]: {
    username: {
      key: "username",
      required: true,
      definition: {
        type: "string" as const,
        title: "Username",
      },
    },
    password: {
      key: "password",
      required: false,
      definition: {
        type: "string" as const,
        title: "Password",
        always_show: true,
        airbyte_secret: true,
      },
    },
  },
  [JWT_AUTHENTICATOR]: {
    secret_key: {
      key: "secret_key",
      required: true,
      definition: {
        type: "string" as const,
        title: "Secret Key",
        airbyte_secret: true,
      },
    },
    algorithm: {
      key: "algorithm",
      required: true,
      definition: {
        type: "string" as const,
        title: "Algorithm",
        always_show: true,
        airbyte_secret: false,
      },
    },
  },
  [OAUTH_AUTHENTICATOR]: {
    client_id: {
      key: "client_id",
      required: true,
      definition: {
        type: "string" as const,
        title: "Client ID",
        airbyte_secret: true,
      },
    },
    client_secret: {
      key: "client_secret",
      required: true,
      definition: {
        type: "string" as const,
        title: "Client secret",
        airbyte_secret: true,
      },
    },
    refresh_token: {
      key: "client_refresh_token",
      required: true,
      definition: {
        type: "string" as const,
        title: "Refresh token",
        airbyte_secret: true,
      },
    },
    refresh_token_updater: {
      access_token_config_path: {
        key: "oauth_access_token",
        required: false,
        definition: {
          type: "string" as const,
          title: "Access token",
          airbyte_secret: true,
          description:
            "The current access token. This field might be overridden by the connector based on the token refresh endpoint response.",
        },
      },
      token_expiry_date_config_path: {
        key: "oauth_token_expiry_date",
        required: false,
        definition: {
          type: "string" as const,
          title: "Token expiry date",
          format: "date-time",
          description:
            "The date the current access token expires in. This field might be overridden by the connector based on the token refresh endpoint response.",
        },
      },
    },
  },
  [DeclarativeOAuthAuthenticatorType]: {
    client_id: {
      key: "client_id",
      required: true,
      definition: {
        type: "string" as const,
        title: "Client ID",
        airbyte_secret: true,
      },
    },
    client_secret: {
      key: "client_secret",
      required: true,
      definition: {
        type: "string" as const,
        title: "Client secret",
        airbyte_secret: true,
      },
    },
    refresh_token: {
      key: "client_refresh_token",
      required: false,
      definition: {
        type: "string" as const,
        title: "Refresh token",
        airbyte_secret: true,
        airbyte_hidden: true,
      },
    },
    access_token: {
      key: "client_access_token",
      required: false,
      definition: {
        type: "string" as const,
        title: "Access token",
        airbyte_secret: true,
        airbyte_hidden: true,
      },
    },
    refresh_token_updater: {
      access_token_config_path: {
        key: "oauth_access_token",
        required: false,
        definition: {
          type: "string" as const,
          title: "Access token",
          airbyte_secret: true,
          airbyte_hidden: true,
          description:
            "The current access token. This field might be overridden by the connector based on the token refresh endpoint response.",
        },
      },
      token_expiry_date_config_path: {
        key: "oauth_token_expiry_date",
        required: false,
        definition: {
          type: "string" as const,
          title: "Token expiry date",
          format: "date-time",
          description:
            "The date the current access token expires in. This field might be overridden by the connector based on the token refresh endpoint response.",
          airbyte_hidden: true,
        },
      },
    },
  },
  [SESSION_TOKEN_AUTHENTICATOR]: {},
};

export const LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME: Record<string, BuilderFormInput> = {
  start_datetime: {
    key: "start_date",
    required: true,
    definition: {
      type: "string",
      title: "Start date",
      format: "date-time",
      pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    },
  },
  end_datetime: {
    key: "end_date",
    required: true,
    definition: {
      type: "string",
      title: "End date",
      format: "date-time",
      pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    },
  },
};
