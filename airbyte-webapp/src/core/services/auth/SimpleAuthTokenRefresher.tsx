import { useEffect } from "react";

import { useSimpleAuthTokenRefresh } from "core/api";

import { useAuthService } from "./AuthContext";

/**
 * When rendered, this component will refresh the auth token every 60 seconds
 */
export const SimpleAuthTokenRefresher: React.FC = () => {
  const { logout } = useAuthService();
  const { error } = useSimpleAuthTokenRefresh();

  useEffect(() => {
    if (error) {
      console.debug("🔑 Failed to refresh simple auth token. User will be logged out.");
      logout?.();
    }
  }, [error, logout]);

  return null;
};
