import { DependencyList, useRef } from "react";
import { useUpdateEffect } from "react-use";

export function useInitService<T>(f: () => T, deps: DependencyList): T {
  const service = useRef<T | null>(null);

  useUpdateEffect(() => {
    if (service.current !== null) {
      service.current = f();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  if (service.current === null) {
    service.current = f();
  }

  return service.current;
}
