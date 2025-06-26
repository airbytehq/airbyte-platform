import { Dispatch, SetStateAction } from "react";
// eslint-disable-next-line no-restricted-imports
import { useLocalStorage as useLocalStorageWithUndefinedBug } from "react-use";

import { BuilderState } from "components/connectorBuilder/types";

import { Theme } from "hooks/theme/useAirbyteTheme";

export interface AssistLocalStorageProject {
  sessionId: string;
}

// Represents all the data we store in localStorage across the airbyte app
interface AirbyteLocalStorage {
  connectorBuilderInputsWarning: boolean;
  connectorBuilderPublishWarning: boolean;
  connectorBuilderRecordView: "json" | "table";
  connectorBuilderLimitWarning: boolean;
  allowlistIpsOpen: boolean;
  airbyteTheme: Theme;
  "airbyte_connector-grid-show-suggested-connectors": boolean;
  "airbyte_show-dev-tools": boolean;
  "airbyte_workspace-in-title": boolean;
  "airbyte_extended-attempts-stats": boolean;
  "airbyte_connection-additional-details": boolean;
  "airbyte_ai-assist-projects": Record<string, AssistLocalStorageProject>;
  "airbyte_last-sso-company-identifier": string;
  "airbyte_connector-builder-modes": Record<string, BuilderState["mode"]>;
  "airbyte_license-check-dismissed-at": string | null;
  "airbyte_connector-builder-advanced-mode": boolean;
}

/*
 * The types for useLocalStorage() are incorrect, as they include `| undefined` even if a non-undefined value is supplied for the initialValue.
 * This function corrects that mistake. This can be removed if this PR is ever merged into that library: https://github.com/streamich/react-use/pull/1438
 */
export const useLocalStorage = <T extends keyof AirbyteLocalStorage>(
  key: T,
  initialValue: AirbyteLocalStorage[T] | undefined,
  options?:
    | {
        raw: true;
      }
    | {
        raw: false;
        serializer: (value: AirbyteLocalStorage[T]) => string;
        deserializer: (value: string) => AirbyteLocalStorage[T];
      }
): [AirbyteLocalStorage[T], Dispatch<SetStateAction<AirbyteLocalStorage[T]>>] => {
  const [storedValue, setStoredValue] = useLocalStorageWithUndefinedBug(key, initialValue, options);

  if (storedValue === undefined) {
    throw new Error("Received an undefined value from useLocalStorage. This should not happen");
  }

  const setStoredValueFixed = setStoredValue as Dispatch<SetStateAction<AirbyteLocalStorage[T]>>;
  return [storedValue, setStoredValueFixed];
};
