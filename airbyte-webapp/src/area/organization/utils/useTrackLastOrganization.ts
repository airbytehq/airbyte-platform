import { useEffect } from "react";

import { useIsCloudApp } from "core/utils/app";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { useCurrentOrganizationId } from "./useCurrentOrganizationId";

/**
 * Tracks the current organization and saves it to localStorage.
 * Cloud-only feature - no-op in OSS edition.
 */
export const useTrackLastOrganization = () => {
  const isCloudApp = useIsCloudApp();
  const currentOrganizationId = useCurrentOrganizationId();
  const [, setLastOrganizationId] = useLocalStorage("airbyte_last-visited-organization-id", "");

  useEffect(() => {
    if (!isCloudApp || !currentOrganizationId) {
      return;
    }

    setLastOrganizationId(currentOrganizationId);
  }, [currentOrganizationId, isCloudApp, setLastOrganizationId]);
};
