import React, { useContext } from "react";
import { Observable } from "rxjs";

import { UserRead, UserReadMetadata } from "core/api/types/AirbyteClient";

export type AuthConfirmPasswordReset = (code: string, newPassword: string) => Promise<void>;

export type AuthLogin = (values: { email: string; password: string }) => Promise<void>;
export type AuthOAuthLogin = (provider: OAuthProviders) => Observable<OAuthLoginState>;

export type AuthChangeName = (name: string) => Promise<void>;

export type AuthSendEmailVerification = () => Promise<void>;
export type AuthVerifyEmail = () => Promise<void>;
export type AuthLogout = () => Promise<void>;

export type OAuthLoginState = "waiting" | "loading" | "done";

export enum AuthProviders {
  GoogleIdentityPlatform = "google_identity_platform",
}

export type OAuthProviders = "github" | "google";

// This override is currently needed because the UserRead type is not consistent between OSS and Cloud
export interface CommonUserRead extends Omit<UserRead, "metadata"> {
  metadata?: UserReadMetadata;
}

export interface AuthContextApi {
  user: CommonUserRead | null;
  inited: boolean;
  emailVerified: boolean;
  loggedOut: boolean;
  provider: string | null;
  getAccessToken?: () => Promise<string | null>;
  login?: AuthLogin;
  updateName?: AuthChangeName;
  confirmPasswordReset?: AuthConfirmPasswordReset;
  sendEmailVerification?: AuthSendEmailVerification;
  verifyEmail?: AuthVerifyEmail;
  logout?: AuthLogout;
}

// The AuthContext is implemented differently in OSS vs. Cloud, but both implementations use the AuthContextApi interface
export const AuthContext = React.createContext<AuthContextApi | null>(null);

export const useCurrentUser = (): CommonUserRead => {
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
