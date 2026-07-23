import React, { createContext, useContext } from "react";

import { type OAuthState } from "./hooks/useConnectorSetupAgentState";

const OAuthContext = createContext<OAuthState | null>(null);

export const OAuthProvider: React.FC<{ value: OAuthState; children: React.ReactNode }> = ({ value, children }) => {
  return <OAuthContext.Provider value={value}>{children}</OAuthContext.Provider>;
};

export const useOAuthContext = (): OAuthState | null => {
  return useContext(OAuthContext);
};
