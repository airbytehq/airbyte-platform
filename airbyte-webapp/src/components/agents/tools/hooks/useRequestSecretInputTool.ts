import { useCallback, useState } from "react";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { type SecretsMap } from "../../types";
import { TOOL_NAMES } from "../toolNames";

export interface UseRequestSecretInputToolParams {
  setSecrets: React.Dispatch<React.SetStateAction<SecretsMap>>;
}

export interface UseRequestSecretInputToolReturn {
  handler: ClientToolHandler;
  isSecretInputActive: boolean;
  secretFieldPath: string[];
  secretFieldName: string;
  isMultiline: boolean;
  submitSecret: (value: string) => void;
}

export const useRequestSecretInputTool = ({
  setSecrets,
}: UseRequestSecretInputToolParams): UseRequestSecretInputToolReturn => {
  const [secretInputState, setSecretInputState] = useState<{
    isActive: boolean;
    fieldPath: string[];
    fieldName: string;
    isMultiline: boolean;
    sendResult: ((result: string) => void) | null;
  }>({
    isActive: false,
    fieldPath: [],
    fieldName: "",
    isMultiline: false,
    sendResult: null,
  });

  const submitSecret = useCallback(
    (value: string) => {
      const pathKey = secretInputState.fieldPath.join(".");

      // Store the secret in the secrets map using the dot-separated path as key
      setSecrets((prevSecrets) => {
        const newSecrets = new Map(prevSecrets);
        newSecrets.set(pathKey, value);
        return newSecrets;
      });

      // Send success confirmation back through the stored callback
      if (secretInputState.sendResult) {
        secretInputState.sendResult("Secret stored successfully");
      }

      // Reset state
      setSecretInputState({
        isActive: false,
        fieldPath: [],
        fieldName: "",
        isMultiline: false,
        sendResult: null,
      });
    },
    [secretInputState, setSecrets]
  );

  const handler: ClientToolHandler = {
    toolName: TOOL_NAMES.REQUEST_SECRET_INPUT,
    execute: (args: unknown, sendResult) => {
      const { field_path, field_name, is_multiline } = args as {
        field_path: string[];
        field_name: string;
        is_multiline: boolean;
      };
      if (field_path && Array.isArray(field_path)) {
        setSecretInputState({
          isActive: true,
          fieldPath: field_path,
          fieldName: field_name || field_path.join("."),
          isMultiline: is_multiline || false,
          sendResult,
        });
      } else {
        console.error("[useRequestSecretInputTool] Invalid field_path in request_secret_input args");
      }
    },
  };

  return {
    handler,
    isSecretInputActive: secretInputState.isActive,
    secretFieldPath: secretInputState.fieldPath,
    secretFieldName: secretInputState.fieldName,
    isMultiline: secretInputState.isMultiline,
    submitSecret,
  };
};
