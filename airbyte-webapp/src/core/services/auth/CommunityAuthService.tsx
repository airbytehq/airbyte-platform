import { PropsWithChildren } from "react";

import { useGetDefaultUser } from "core/api";

import { AuthContext } from "./AuthContext";

export const CommunityAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  // In Community, the getUser endpoint does not require an access token
  const defaultUser = useGetDefaultUser({ getAccessToken: () => Promise.resolve(null) });

  return (
    <AuthContext.Provider
      value={{
        user: defaultUser,
        inited: true,
        emailVerified: false,
        providers: [],
        loggedOut: false,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
