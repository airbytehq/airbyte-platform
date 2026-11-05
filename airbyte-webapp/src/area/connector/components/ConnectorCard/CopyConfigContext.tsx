import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

type GetConfigJson = () => string;

interface CopyConfigContextValue {
  getConfigJson?: GetConfigJson;
  registerGetConfigJson: (getConfigJson: GetConfigJson | undefined) => void;
}

const CopyConfigContext = createContext<CopyConfigContextValue | null>(null);

/**
 * Lets a connector form hand its live "Copy JSON" content to a component that renders outside the form, such as the
 * title block on the source and destination item pages.
 */
export const CopyConfigProvider: React.FC<React.PropsWithChildren> = ({ children }) => {
  const [getConfigJson, setGetConfigJson] = useState<GetConfigJson>();
  const registerGetConfigJson = useCallback(
    (nextGetConfigJson: GetConfigJson | undefined) => setGetConfigJson(() => nextGetConfigJson),
    []
  );
  const value = useMemo(() => ({ getConfigJson, registerGetConfigJson }), [getConfigJson, registerGetConfigJson]);

  return <CopyConfigContext.Provider value={value}>{children}</CopyConfigContext.Provider>;
};

export const useSharedConfigJson = (): GetConfigJson | undefined => useContext(CopyConfigContext)?.getConfigJson;

/** `getConfigJson` must keep its identity between renders. Without a provider, this hook does nothing. */
export const useRegisterConfigJson = (getConfigJson: GetConfigJson | undefined) => {
  const registerGetConfigJson = useContext(CopyConfigContext)?.registerGetConfigJson;

  useEffect(() => {
    registerGetConfigJson?.(getConfigJson);
    return () => registerGetConfigJson?.(undefined);
  }, [getConfigJson, registerGetConfigJson]);
};
