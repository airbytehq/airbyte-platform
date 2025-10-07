/**
 * Utility functions for SSO test URL parameter handling
 */

/**
 * Check if the current URL represents an SSO test callback
 * @returns true if this is an SSO test callback
 */
export const isSsoTestCallback = (): boolean => {
  const search = window.location.search;
  return new URLSearchParams(search).get("sso_test") === "true";
};

/**
 * Get the realm parameter from an SSO test callback URL
 * @returns the realm parameter value, or null if not present
 */
export const getSsoTestRealm = (): string | null => {
  const search = window.location.search;
  return new URLSearchParams(search).get("realm");
};
