import { useMemo } from "react";

import { useListOrganizationsByUser } from "core/api";
import { useCurrentUser } from "core/services/auth";
import { useIsCloudApp } from "core/utils/app";
import { useLocalStorage } from "core/utils/useLocalStorage";

/**
 * Returns the last visited organization ID if the user still has access to it.
 * Cloud-only feature - returns undefined in OSS edition.
 */
export const useLastVisitedOrganizationId = (): string | undefined => {
  const isCloudApp = useIsCloudApp();
  const [lastOrganizationId] = useLocalStorage("airbyte_last-visited-organization-id", "");
  const { userId } = useCurrentUser();
  const { organizations } = useListOrganizationsByUser(
    { userId },
    { enabled: isCloudApp && Boolean(lastOrganizationId) }
  );

  return useMemo(() => {
    if (!isCloudApp || !lastOrganizationId) {
      return undefined;
    }

    const hasAccess = organizations.some((org) => org.organizationId === lastOrganizationId);

    return hasAccess ? lastOrganizationId : undefined;
  }, [isCloudApp, lastOrganizationId, organizations]);
};
