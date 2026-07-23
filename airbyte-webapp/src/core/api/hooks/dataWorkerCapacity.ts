import { useCallback } from "react";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { getWorkspaceDataWorkerAvailability } from "core/api/generated/AirbyteClient";
import { useRequestOptions } from "core/api/useRequestOptions";

export const useGetDataWorkerAvailability = () => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();
  const organizationId = useCurrentOrganizationId();

  return useCallback(
    () => getWorkspaceDataWorkerAvailability({ workspaceId, organizationId }, requestOptions),
    [workspaceId, organizationId, requestOptions]
  );
};
