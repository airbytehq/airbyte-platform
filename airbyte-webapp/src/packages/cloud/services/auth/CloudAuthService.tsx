import { useQueryClient } from "@tanstack/react-query";
import React, { PropsWithChildren, useCallback, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { LoadingPage } from "components";

import { useUpdateUser } from "core/api/cloud";
import { AuthProvider } from "core/api/types/AirbyteClient";
import { AuthContext, AuthContextApi } from "core/services/auth";

import { useKeycloakService } from "./KeycloakService";

// Checks for a valid auth session with keycloak and returns the user if found.
export const CloudAuthService: React.FC<PropsWithChildren> = ({ children }) => {
  const [logoutInProgress, setLogoutInProgress] = useState(false);
  const queryClient = useQueryClient();
  const { mutateAsync: updateAirbyteUser } = useUpdateUser();
  const keycloakAuth = useKeycloakService();
  const navigate = useNavigate();

  const logout = useCallback(async () => {
    setLogoutInProgress(true);
    if (keycloakAuth.isAuthenticated) {
      await keycloakAuth.signout();
    }
    setLogoutInProgress(false);
    navigate("/");
    queryClient.removeQueries();
  }, [keycloakAuth, navigate, queryClient]);

  const authContextValue = useMemo<AuthContextApi>(() => {
    // The context value for an authenticated Keycloak user.
    if (keycloakAuth.isAuthenticated) {
      return {
        isAuthenticated: true,
        inited: true,
        user: keycloakAuth.airbyteUser,
        authProvider: AuthProvider.keycloak,
        displayName: keycloakAuth.keycloakUser?.profile.name ?? null,
        emailVerified: keycloakAuth.keycloakUser?.profile.email_verified ?? false,
        email: keycloakAuth.keycloakUser?.profile.email ?? null,
        getAccessToken: () => Promise.resolve(keycloakAuth.accessTokenRef?.current),
        updateName: async (name: string) => {
          const user = keycloakAuth.airbyteUser;
          if (!user) {
            throw new Error("Cannot change name, airbyteUser is null");
          }
          await updateAirbyteUser({
            userUpdate: { userId: user.userId, name },
            getAccessToken: async () => keycloakAuth.accessTokenRef?.current ?? "",
          }).then(() => {
            keycloakAuth.updateAirbyteUser({ ...user, name });
          });
        },
        logout,
        loggedOut: false,
        providers: null,
        provider: keycloakAuth.isSso
          ? "sso"
          : (keycloakAuth.keycloakUser?.profile.identity_provider as string | undefined) ?? "none",
      };
    }
    // The context value for an unauthenticated user
    return {
      isAuthenticated: false,
      user: null,
      inited: keycloakAuth.didInitialize,
      userId: null,
      emailVerified: false,
      loggedOut: true,
      providers: null,
      provider: null,
    };
  }, [keycloakAuth, logout, updateAirbyteUser]);

  if (logoutInProgress) {
    return <LoadingPage />;
  }

  return <AuthContext.Provider value={authContextValue}>{children}</AuthContext.Provider>;
};
