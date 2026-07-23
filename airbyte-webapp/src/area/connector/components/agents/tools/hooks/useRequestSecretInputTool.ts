import { useCallback, useState } from "react";
import { useFormContext } from "react-hook-form";

import { makeConnectionConfigurationPath } from "area/connector/components/ConnectorForm/utils";

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

interface SecretInputRequest {
  fieldPath: string[];
  fieldName: string;
  isMultiline: boolean;
  sendResult: (result: string) => void;
}

// Stable reference for the empty-path fallback so the returned value keeps a
// constant identity across renders when the queue is empty. Returning a fresh
// `[]` here would change identity every render and drive consumers' effects
// (e.g. ConnectorSetupAgentTools) into an update loop.
const EMPTY_PATH: string[] = [];

export const useRequestSecretInputTool = (
  addTouchedSecretField?: (path: string) => void
): UseRequestSecretInputToolReturn => {
  const { setValue } = useFormContext();
  const [queue, setQueue] = useState<SecretInputRequest[]>([]);

  const current = queue[0] as SecretInputRequest | undefined;

  const submitSecret = useCallback(
    (value: string) => {
      if (!current) {
        return;
      }

      const fullPath = makeConnectionConfigurationPath(current.fieldPath);

      setValue(fullPath, value, {
        shouldDirty: true,
        shouldValidate: true,
        shouldTouch: true,
      });

      if (addTouchedSecretField) {
        addTouchedSecretField(fullPath);
      }

      current.sendResult("Secret stored successfully");

      setQueue((prev) => prev.slice(1));
    },
    [current, setValue, addTouchedSecretField]
  );

  const dismissSecret = useCallback(
    (reason?: string) => {
      if (!current) {
        return;
      }

      current.sendResult(reason || "cancelled!");

      setQueue((prev) => prev.slice(1));
    },
    [current]
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
        setQueue((prev) => [
          ...prev,
          {
            fieldPath: field_path,
            fieldName: field_name || field_path.join("."),
            isMultiline: is_multiline || false,
            sendResult,
          },
        ]);
      } else {
        console.error("[useRequestSecretInputTool] Invalid field_path in request_secret_input args");
      }
    },
  };

  return {
    handler,
    isSecretInputActive: current !== undefined,
    secretFieldPath: current?.fieldPath ?? EMPTY_PATH,
    secretFieldName: current?.fieldName ?? "",
    isMultiline: current?.isMultiline ?? false,
    submitSecret,
    dismissSecret,
  };
};
