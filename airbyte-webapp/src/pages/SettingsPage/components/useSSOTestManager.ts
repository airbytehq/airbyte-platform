import { UserManager, WebStorageStateStore } from "oidc-client-ts";

import { buildConfig } from "core/config";

import { getSsoTestRealm, isSsoTestCallback } from "./ssoTestUtils";

/**
 * Creates a UserManager for SSO testing with the realm from query params.
 * Uses localStorage with sso_test. prefix to avoid conflicts with CloudAuthService.
 * Returns null if not in an SSO test context.
 */
export function useSSOTestManager(): UserManager | null {
  const realm = getSsoTestRealm();

  if (!isSsoTestCallback() || !realm) {
    return null;
  }

  return createSSOTestManager(realm);
}

/**
 * Creates a UserManager for initiating SSO testing with a specific realm.
 * Used when the user clicks "Test Configuration" to start the OAuth flow.
 */
export function createSSOTestManager(realm: string): UserManager {
  const searchParams = new URLSearchParams(window.location.search);
  searchParams.set("sso_test", "true");
  searchParams.set("realm", realm);
  const redirectUri = `${window.location.origin}${window.location.pathname}?${searchParams.toString()}`;

  return new UserManager({
    userStore: new WebStorageStateStore({
      store: window.localStorage,
      prefix: "sso_test.",
    }),
    authority: `${buildConfig.keycloakBaseUrl}/auth/realms/${realm}`,
    client_id: "airbyte-webapp",
    redirect_uri: redirectUri,
    scope: "openid profile email",
  });
}
