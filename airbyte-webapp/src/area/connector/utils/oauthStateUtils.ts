/**
 * Utility functions for OAuth state parameter handling.
 * Extracted for testability.
 */

/**
 * Extracts a customer redirect URL from an OAuth state parameter.
 * Used for cross-origin OAuth flows (e.g., Shopify OAuth initiated from Sonar).
 *
 * State format: randomState|redirect=base64EncodedUrl
 *
 * @param state - The OAuth state parameter
 * @returns The decoded redirect URL, or null if not present or invalid
 */
export const extractCustomerRedirectUrl = (state: string | null): string | null => {
  if (!state) {
    return null;
  }
  const parts = state.split("|");
  for (const part of parts) {
    if (part.startsWith("redirect=")) {
      const encodedUrl = part.substring("redirect=".length);
      if (!encodedUrl) {
        return null;
      }
      try {
        // Handle URL-safe base64: replace - with + and _ with /
        return atob(encodedUrl.replace(/-/g, "+").replace(/_/g, "/"));
      } catch {
        return null;
      }
    }
  }
  return null;
};
