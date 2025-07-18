import cloneDeep from "lodash/cloneDeep";
import { useCallback } from "react";
import { useFormContext } from "react-hook-form";

import {
  ApiKeyAuthenticatorType,
  BasicHttpAuthenticatorType,
  BearerAuthenticatorType,
  OAuthAuthenticatorType,
  JwtAuthenticatorType,
  HttpRequesterAuthenticator,
  ConnectorManifest,
  OAuthConfigSpecificationOauthConnectorInputSpecification,
  OAuthAuthenticatorRefreshTokenUpdater,
} from "core/api/types/ConnectorManifest";

import { OAUTH_INPUT_SPEC_PATH } from "./Builder/BuilderDeclarativeOAuth";
import { convertToBuilderFormInputs, convertToConnectionSpecification } from "./Builder/InputsView";
import { INPUT_REFERENCE_KEYWORD, VALID_AUTHENTICATOR_TYPES } from "./constants";
import { AuthenticatorType, BuilderFormInput } from "./types";
import { extractInterpolatedConfigPath } from "./utils";

export const isHttpRequesterAuthenticator = (authenticator: unknown): authenticator is HttpRequesterAuthenticator => {
  return (
    typeof authenticator === "object" &&
    authenticator !== null &&
    "type" in authenticator &&
    typeof authenticator.type === "string" &&
    (VALID_AUTHENTICATOR_TYPES as unknown as string[]).includes(authenticator.type)
  );
};

export const useAuthenticatorInputs = () => {
  const { getValues, setValue } = useFormContext();

  const updateUserInputsForAuth = useCallback(
    (oldAuth: HttpRequesterAuthenticator | undefined, newAuth: HttpRequesterAuthenticator | undefined) => {
      if (!oldAuth && !newAuth) {
        return;
      }

      if (oldAuth?.type === newAuth?.type) {
        return;
      }

      const spec = getValues("manifest.spec");
      const currentInputs = convertToBuilderFormInputs(spec);

      const inputsToRemove = oldAuth?.type ? Object.values(AUTO_CREATED_INPUTS_BY_AUTH_TYPE[oldAuth.type] ?? {}) : [];
      if (oldAuth?.type === OAuthAuthenticatorType.OAuthAuthenticator && oldAuth?.refresh_token_updater) {
        inputsToRemove.push(TOKEN_UPDATER_INPUT);
      }
      const manifest = getValues("manifest");
      const inputKeysToRemove = inputsToRemove
        .map((input) => input.key)
        .filter((key) => !inputKeyIsReferencedInManifest(manifest, key));

      const declarativeOAuthInputSpec = getValues(OAUTH_INPUT_SPEC_PATH);
      const fieldToNewInput = newAuth?.type ? AUTO_CREATED_INPUTS_BY_AUTH_TYPE[newAuth.type] ?? {} : {};
      const inputsToAdd = modifyInputsForDeclarativeOAuth(Object.values(fieldToNewInput), !!declarativeOAuthInputSpec);

      const newInputs = mergeInputsPreferExisting(
        currentInputs.filter((input) => !inputKeysToRemove.includes(input.key)),
        inputsToAdd
      );
      setValue("manifest.spec.connection_specification", convertToConnectionSpecification(newInputs));
    },
    [getValues, setValue]
  );

  const updateUserInputsForDeclarativeOAuth = useCallback(
    (value: OAuthConfigSpecificationOauthConnectorInputSpecification | undefined) => {
      const spec = getValues("manifest.spec");
      const currentInputs = convertToBuilderFormInputs(spec);

      const newInputs = modifyInputsForDeclarativeOAuth(currentInputs, !!value);
      setValue("manifest.spec.connection_specification", convertToConnectionSpecification(newInputs));
    },
    [getValues, setValue]
  );

  const updateUserInputsForTokenUpdater = useCallback(
    (path: string, value: OAuthAuthenticatorRefreshTokenUpdater | undefined) => {
      const spec = getValues("manifest.spec");
      const currentInputs = convertToBuilderFormInputs(spec);

      if (!value) {
        const manifest = getValues("manifest");
        if (inputKeyIsReferencedInManifest(manifest, TOKEN_UPDATER_INPUT.key)) {
          return;
        }
        const newInputs = currentInputs.filter((input) => input.key !== TOKEN_UPDATER_INPUT.key);
        setValue("manifest.spec.connection_specification", convertToConnectionSpecification(newInputs));
        return;
      }

      const declarativeOAuthInputSpec = getValues(OAUTH_INPUT_SPEC_PATH);
      const newInputs = mergeInputsPreferExisting(
        currentInputs,
        modifyInputsForDeclarativeOAuth([TOKEN_UPDATER_INPUT], !!declarativeOAuthInputSpec)
      );
      setValue("manifest.spec.connection_specification", convertToConnectionSpecification(newInputs));

      const oAuthAuthenticator = getValues(path.split(".").slice(0, -1).join("."));
      const accessTokenConfigPath = extractInterpolatedConfigPath(oAuthAuthenticator.access_token_value);
      const refreshTokenConfigPath = extractInterpolatedConfigPath(oAuthAuthenticator.refresh_token);
      setValue(`${path}.access_token_config_path`, accessTokenConfigPath.split("."));
      setValue(`${path}.refresh_token_config_path`, refreshTokenConfigPath.split("."));
      setValue(`${path}.token_expiry_date_config_path`, [TOKEN_UPDATER_INPUT.key]);
    },
    [getValues, setValue]
  );

  return { updateUserInputsForAuth, updateUserInputsForDeclarativeOAuth, updateUserInputsForTokenUpdater };
};

const mergeInputsPreferExisting = (existingInputs: BuilderFormInput[], newInputs: BuilderFormInput[]) => {
  const filteredNewInputs = newInputs.filter(
    (input) => !existingInputs.some((existingInput) => existingInput.key === input.key)
  );
  return [...existingInputs, ...filteredNewInputs];
};

const inputKeyIsReferencedInManifest = (manifest: ConnectorManifest, key: string) => {
  // Supports:
  // - {{ config.key }}
  // - {{ config['key'] }}
  // - {{ config["key"] }}
  // - {{ config["""key"""] }}   (arbitrary number of quotes)
  // - {{ config[\"key\"]}}      (to support stringified json containing quotes)
  // - Whitespace between any of the parts
  const inputReferenceRegex = new RegExp(
    String.raw`{{\s*${INPUT_REFERENCE_KEYWORD}\s*(\.\s*${key}|\[([\s'"\\]+)${key}([\s'"\\]+)\])\s*}}`
  );
  const stringifiedManifest = JSON.stringify(manifest);
  return inputReferenceRegex.test(stringifiedManifest);
};

const modifyInputsForDeclarativeOAuth = (inputs: BuilderFormInput[], declarativeOAuthEnabled: boolean) => {
  const newInputs = cloneDeep(inputs);
  if (declarativeOAuthEnabled) {
    return newInputs.map((input) => {
      if (
        input.key === AUTO_CREATED_INPUTS_BY_AUTH_TYPE[OAuthAuthenticatorType.OAuthAuthenticator]?.refresh_token?.key
      ) {
        return {
          ...input,
          required: false,
          definition: {
            ...input.definition,
            airbyte_hidden: true,
          },
        };
      }
      if (
        input.key ===
        AUTO_CREATED_INPUTS_BY_AUTH_TYPE[OAuthAuthenticatorType.OAuthAuthenticator]?.access_token_value?.key
      ) {
        return {
          ...input,
          definition: {
            ...input.definition,
            airbyte_hidden: true,
          },
        };
      }
      if (input.key === TOKEN_UPDATER_INPUT.key) {
        return {
          ...input,
          definition: {
            ...input.definition,
            airbyte_hidden: true,
          },
        };
      }
      return input;
    });
  }

  return newInputs.map((input) => {
    if (input.key === AUTO_CREATED_INPUTS_BY_AUTH_TYPE[OAuthAuthenticatorType.OAuthAuthenticator]?.refresh_token?.key) {
      return {
        ...input,
        required: true,
        definition: {
          ...input.definition,
          airbyte_hidden: undefined,
        },
      };
    }
    if (input.key === AUTO_CREATED_INPUTS_BY_AUTH_TYPE[OAuthAuthenticatorType.OAuthAuthenticator]?.access_token?.key) {
      return {
        ...input,
        definition: {
          ...input.definition,
          airbyte_hidden: undefined,
        },
      };
    }
    if (input.key === TOKEN_UPDATER_INPUT.key) {
      return {
        ...input,
        definition: {
          ...input.definition,
          airbyte_hidden: undefined,
        },
      };
    }
    return input;
  });
};

export const AUTO_CREATED_INPUTS_BY_AUTH_TYPE: Partial<Record<AuthenticatorType, Record<string, BuilderFormInput>>> = {
  [ApiKeyAuthenticatorType.ApiKeyAuthenticator]: {
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
  [BearerAuthenticatorType.BearerAuthenticator]: {
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
  [BasicHttpAuthenticatorType.BasicHttpAuthenticator]: {
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
  [JwtAuthenticatorType.JwtAuthenticator]: {
    secret_key: {
      key: "secret_key",
      required: true,
      definition: {
        type: "string" as const,
        title: "Secret Key",
        airbyte_secret: true,
      },
    },
  },
  [OAuthAuthenticatorType.OAuthAuthenticator]: {
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
    access_token_value: {
      key: "client_access_token",
      required: false,
      definition: {
        type: "string" as const,
        title: "Access token",
        airbyte_secret: true,
      },
    },
  },
};

const TOKEN_UPDATER_INPUT: BuilderFormInput = {
  key: "oauth_token_expiry_date",
  required: false,
  definition: {
    type: "string" as const,
    title: "Token expiry date",
    format: "date-time",
    description:
      "The date the current access token expires in. This field might be overridden by the connector based on the OAuth API response.",
    airbyte_hidden: true,
  },
};
