import { PropsWithChildren } from "react";

import { useGetDefaultUser } from "core/api";

import { AuthContext } from "./AuthContext";

// This is a static auth service in case the auth mode of the Airbyte instance is set to "none"
export const NoAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const defaultUser = useGetDefaultUser();

  return (
    <AuthContext.Provider
      value={{
        authType: "none",
        applicationSupport: "none",
        user: defaultUser,
        inited: true,
        emailVerified: false,
        provider: null,
        loggedOut: false,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
