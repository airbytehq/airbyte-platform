import { useEffect, useMemo, useRef } from "react";

const usePrevious = (dependencies: readonly unknown[]) => {
  const ref = useRef(dependencies);
  useEffect(() => {
    ref.current = dependencies;
  });
  return ref.current;
};

export const useMemoDebug = <T>(factory: () => T, deps: readonly unknown[], hookName?: string): T => {
  const previousDeps = usePrevious(deps);
  return useMemo(() => {
    console.log(`%c${hookName || "useMemoDebug"} calculated new value`, "background-color: #0f766e; color: #FFF");
    const changes = deps.map((dep, index) => {
      return {
        "Changed?": dep !== previousDeps[index] ? "🆕" : "⬜",
        "Old Value": previousDeps[index],
        "New Value": dep,
      };
    });
    console.table(changes);
    return factory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
};
