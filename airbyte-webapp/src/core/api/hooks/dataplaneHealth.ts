import { useQuery } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { listDataplaneHealth } from "../generated/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const useListDataplaneHealth = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();

  const query = useQuery(
    ["dataplaneHealth", organizationId],
    async () => {
      const response = await listDataplaneHealth({ organizationId }, requestOptions);
      return response.dataplanes ?? [];
    },
    {
      suspense: true,
      refetchInterval: 5000, // Poll every 5 seconds
    }
  );

  return {
    data: query.data!,
    dataUpdatedAt: query.dataUpdatedAt,
  };
};
