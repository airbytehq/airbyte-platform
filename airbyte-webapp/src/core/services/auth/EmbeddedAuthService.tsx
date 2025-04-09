import { PropsWithChildren } from "react";
import { useSearchParams } from "react-router-dom";

import { AuthContext } from "./AuthContext";

/**
 * This is an auth service for the embedded widget. The auth scheme used by the endpoints
 * available in the embedded widget use scoped tokens that will be read starting in https://github.com/airbytehq/airbyte-platform-internal/pull/15796.
 * This auth service is used to provide an empty user object to the embedded widget to fulfill the AuthContext interface.
 */
export const EmbeddedAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  // this will be updated when https://github.com/airbytehq/airbyte-platform-internal/pull/15796 is merged to use an event listener instead of a search param
  const [searchParams] = useSearchParams();
  const scopedAuthToken = searchParams.get("auth");

  return (
    <AuthContext.Provider
      value={{
        authType: "embedded",
        applicationSupport: "none",
        user: null,
        inited: !!scopedAuthToken,
        emailVerified: false,
        provider: null,
        loggedOut: !!scopedAuthToken,
        getAccessToken: () => Promise.resolve(scopedAuthToken),
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
