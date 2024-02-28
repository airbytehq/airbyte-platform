import { Dispatch, SetStateAction } from "react";
// eslint-disable-next-line no-restricted-imports
import { useLocalStorage as useLocalStorageWithUndefinedBug } from "react-use";

import { BuilderState } from "components/connectorBuilder/types";

import { SupportLevel } from "core/api/types/AirbyteClient";
import { Theme } from "hooks/theme/useAirbyteTheme";

// Represents all the data we store in localStorage across the airbyte app
interface AirbyteLocalStorage {
  "exp-speedy-connection-timestamp": string;
  connectorBuilderEditorView: BuilderState["mode"];
  connectorBuilderInputsWarning: boolean;
  connectorBuilderRecordView: "json" | "table";
  connectorBuilderLimitWarning: boolean;
  allowlistIpsOpen: boolean;
  airbyteTheme: Theme;
  "airbyte_connector-grid-support-level-filter": SupportLevel[];
  "airbyte_connector-grid-show-suggested-connectors": boolean;
  "airbyte_show-dev-tools": boolean;
  "airbyte_workspace-in-title": boolean;
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
