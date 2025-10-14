import { UserManager, WebStorageStateStore } from "oidc-client-ts";

import { buildConfig } from "core/config";

/**
 * Creates a UserManager for SSO testing with a specific realm.
 *
 * This is used for testing SSO configurations by initiating an OAuth flow with the identity provider.
 * The UserManager uses separate localStorage storage (with sso_test. prefix) for both user and state
 * data to prevent OAuth state conflicts with the main CloudAuthService UserManager.
 *
 * @param realm - The Keycloak realm identifier (company identifier) to test
 * @returns A configured UserManager instance for SSO testing
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
    stateStore: new WebStorageStateStore({
      store: window.localStorage,
      prefix: "sso_test.",
    }),
    authority: `${buildConfig.keycloakBaseUrl}/auth/realms/${realm}`,
    client_id: "airbyte-webapp",
    redirect_uri: redirectUri,
    scope: "openid profile email",
  });
}
