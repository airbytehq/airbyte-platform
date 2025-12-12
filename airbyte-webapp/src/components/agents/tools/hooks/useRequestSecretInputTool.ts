import { useCallback, useState } from "react";
import { useFormContext } from "react-hook-form";

import { makeConnectionConfigurationPath } from "views/Connector/ConnectorForm/utils";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { TOOL_NAMES } from "../toolNames";

export interface UseRequestSecretInputToolReturn {
  handler: ClientToolHandler;
  isSecretInputActive: boolean;
  secretFieldPath: string[];
  secretFieldName: string;
  isMultiline: boolean;
  submitSecret: (value: string) => void;
  dismissSecret: (reason?: string) => void;
}

interface SecretInputState {
  isActive: boolean;
  fieldPath: string[];
  fieldName: string;
  isMultiline: boolean;
  sendResult: ((result: string) => void) | null;
}

const DEFAULT_SECRET_INPUT_STATE: SecretInputState = {
  isActive: false,
  fieldPath: [],
  fieldName: "",
  isMultiline: false,
  sendResult: null,
};

export const useRequestSecretInputTool = (
  addTouchedSecretField?: (path: string) => void
): UseRequestSecretInputToolReturn => {
  const { setValue } = useFormContext();
  const [secretInputState, setSecretInputState] = useState<SecretInputState>(DEFAULT_SECRET_INPUT_STATE);

  const submitSecret = useCallback(
    (value: string) => {
      const fullPath = makeConnectionConfigurationPath(secretInputState.fieldPath);

      // Store actual secret value in form
      setValue(fullPath, value, {
        shouldDirty: true,
        shouldValidate: true,
        shouldTouch: true,
      });

      // Track this field as touched to prevent overwrites
      if (addTouchedSecretField) {
        addTouchedSecretField(fullPath);
      }

      // Send success confirmation back through the stored callback
      if (secretInputState.sendResult) {
        secretInputState.sendResult("Secret stored successfully");
      }

      // Reset state
      setSecretInputState(DEFAULT_SECRET_INPUT_STATE);
    },
    [secretInputState, setValue, addTouchedSecretField]
  );

  const dismissSecret = useCallback(
    (reason?: string) => {
      // Send dismissal message to LLM
      if (secretInputState.sendResult) {
        const message = reason || "cancelled!";
        secretInputState.sendResult(message);
      }

      // Reset state
      setSecretInputState(DEFAULT_SECRET_INPUT_STATE);
    },
    [secretInputState]
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
    dismissSecret,
  };
};
