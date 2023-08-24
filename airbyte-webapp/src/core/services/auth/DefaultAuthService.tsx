import { PropsWithChildren } from "react";

import { useGetDefaultUser } from "core/api";

import { AuthContext } from "./AuthContext";

export const DefaultAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const defaultUser = useGetDefaultUser();

  return (
    <AuthContext.Provider
      value={{
        user: defaultUser,
        inited: true,
        emailVerified: false,
        isLoading: false,
        providers: [],
        loggedOut: false,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
