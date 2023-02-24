import React, { Dispatch, SetStateAction, useContext } from "react";
import { useLocalStorage } from "react-use";

import { EditorView } from "components/connectorBuilder/types";

interface LocalStorageContext {
  storedEditorView: EditorView;
  setStoredEditorView: (view: EditorView) => void;
}

export const ConnectorBuilderLocalStorageContext = React.createContext<LocalStorageContext | null>(null);

export const ConnectorBuilderLocalStorageProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [storedEditorView, setStoredEditorView] = useLocalStorageFixed<EditorView>("connectorBuilderEditorView", "ui");

  const ctx = {
    storedEditorView,
    setStoredEditorView,
  };

  return (
    <ConnectorBuilderLocalStorageContext.Provider value={ctx}>{children}</ConnectorBuilderLocalStorageContext.Provider>
  );
};

export const useConnectorBuilderLocalStorage = (): LocalStorageContext => {
  const connectorBuilderLocalStorage = useContext(ConnectorBuilderLocalStorageContext);
  if (!connectorBuilderLocalStorage) {
    throw new Error("useConnectorBuilderLocalStorage must be used within a ConnectorBuilderLocalStorageProvider.");
  }

  return connectorBuilderLocalStorage;
};

/*
 * The types for useLocalStorage() are incorrect, as they include `| undefined` even if a non-undefined value is supplied for the initialValue.
 * This function corrects that mistake. This can be removed if this PR is ever merged into that library: https://github.com/streamich/react-use/pull/1438
 */
const useLocalStorageFixed = <T,>(
  key: string,
  initialValue: T,
  options?:
    | {
        raw: true;
      }
    | {
        raw: false;
        serializer: (value: T) => string;
        deserializer: (value: string) => T;
      }
): [T, Dispatch<SetStateAction<T>>] => {
  const [storedValue, setStoredValue] = useLocalStorage(key, initialValue, options);

  if (storedValue === undefined) {
    throw new Error("Received an undefined value from useLocalStorage. This should not happen");
  }

  const setStoredValueFixed = setStoredValue as Dispatch<SetStateAction<T>>;
  return [storedValue, setStoredValueFixed];
};
