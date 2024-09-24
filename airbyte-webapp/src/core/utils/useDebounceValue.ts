import { useState } from "react";
import { useDebounce } from "react-use";

/**
 * Debounces the value and returns the debounced value.
 * @param value - The value to debounce.
 * @param debounceTime - The time in milliseconds to debounce the value.
 * @returns - The debounced value.
 */
export const useDebounceValue = <T>(value: T, debounceTime: number) => {
  const [debouncedValue, setDebouncedValue] = useState<T>(value);
  useDebounce(
    () => {
      setDebouncedValue(value);
    },
    debounceTime,
    [value]
  );
  return debouncedValue;
};
