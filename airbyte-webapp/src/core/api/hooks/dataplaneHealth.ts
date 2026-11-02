import { useQuery } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { listDataplaneHealth } from "../generated/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

interface UseListDataplaneHealthOptions {
  refetchInterval: number;
}

export const useListDataplaneHealth = ({ refetchInterval }: UseListDataplaneHealthOptions) => {
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
      refetchInterval,
    }
  );

  return {
    data: query.data!,
    dataUpdatedAt: query.dataUpdatedAt,
  };
};
