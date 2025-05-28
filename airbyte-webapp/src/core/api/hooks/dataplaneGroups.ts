import { useCallback } from "react";

import { useCurrentWorkspace } from "./workspaces";
import { listDataplaneGroups } from "../generated/AirbyteClient";
import { DataplaneGroupRead } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const useListDataplaneGroups = () => {
  const { organizationId } = useCurrentWorkspace();
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(["dataplaneGroups", organizationId], async () => {
    const response = await listDataplaneGroups({ organization_id: organizationId }, requestOptions);
    return response.dataplaneGroups ?? [];
  });
};

export const useGetDataplaneGroup = (): {
  getDataplaneGroup: (dataplaneGroupId?: string) => DataplaneGroupRead | undefined;
} => {
  const dataPlaneGroups = useListDataplaneGroups();

  return {
    getDataplaneGroup: useCallback(
      (dataplaneGroupId?: string) =>
        dataPlaneGroups.find((dataplaneGroup) => dataplaneGroup.dataplane_group_id === dataplaneGroupId),
      [dataPlaneGroups]
    ),
  };
};
