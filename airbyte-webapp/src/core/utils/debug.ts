import { useEffect } from "react";

/**
 * Utility to make a value via a getter available on the window object for debugging.
 */
export const useDebugVariable = (name: string, getter: () => unknown): void => {
  useEffect(() => {
    Object.defineProperty(window, name, {
      get: getter,
      configurable: true,
    });
    return () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      delete (window as any)[name];
    };
    // We don't want to reattach this if the getter changes, to make sure consumers
    // of this don't need to useCallback the argument.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name]);
};
