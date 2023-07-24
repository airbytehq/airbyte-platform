import { Dispatch, SetStateAction } from "react";
import { useLocalStorage } from "react-use";

/*
 * The types for useLocalStorage() are incorrect, as they include `| undefined` even if a non-undefined value is supplied for the initialValue.
 * This function corrects that mistake. This can be removed if this PR is ever merged into that library: https://github.com/streamich/react-use/pull/1438
 */
export const useLocalStorageFixed = <T>(
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
