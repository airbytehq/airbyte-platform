import React, { useContext } from "react";

import { UserRead } from "core/api/types/AirbyteClient";

export type AuthChangeName = (name: string) => Promise<void>;
export type AuthGetAccessToken = () => Promise<string | null>;
export type AuthLogout = () => Promise<void>;

export interface AuthContextApi {
  user: UserRead | null;
  inited: boolean;
  emailVerified: boolean;
  loggedOut: boolean;
  provider: string | null;
  getAccessToken?: AuthGetAccessToken;
  updateName?: AuthChangeName;
  logout?: AuthLogout;
}

// The AuthContext is implemented differently in Community vs. Self-Managed Enterprise vs. Cloud, but all implementations must fulfill the AuthContextApi interface
export const AuthContext = React.createContext<AuthContextApi | null>(null);

export const useCurrentUser = (): UserRead => {
  const { user } = useAuthService();
  if (!user) {
    throw new Error("useCurrentUser must be used only within authorised flow");
  }

  return user;
};

export const useAuthService = (): AuthContextApi => {
  const authService = useContext(AuthContext);
  if (!authService) {
    throw new Error("useAuthService must be used within a AuthenticationService.");
  }

  return authService;
};
