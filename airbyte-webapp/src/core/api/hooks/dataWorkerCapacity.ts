import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import { useIntl } from "react-intl";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  getWorkspaceDataWorkerAvailability,
  listDataWorkerAllocations,
  reallocateDataWorkerCapacity,
} from "core/api/generated/AirbyteClient";
import { useRequestOptions } from "core/api/useRequestOptions";
import { useSuspenseQuery } from "core/api/useSuspenseQuery";
import { useNotificationService } from "core/services/Notification";

import { SCOPE_ORGANIZATION } from "../scopes";
import { DataWorkerAllocationListResponse } from "../types/AirbyteClient";

export const dataWorkerCapacityKeys = {
  all: [SCOPE_ORGANIZATION, "dataWorkerCapacity"] as const,
  allocations: () => [...dataWorkerCapacityKeys.all, "allocations"] as const,
  allocationList: (organizationId: string) => [...dataWorkerCapacityKeys.allocations(), organizationId] as const,
};

export const useGetDataWorkerAvailability = () => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();
  const organizationId = useCurrentOrganizationId();

  return useCallback(
    () => getWorkspaceDataWorkerAvailability({ workspaceId, organizationId }, requestOptions),
    [workspaceId, organizationId, requestOptions]
  );
};

/**
 * The Data Worker capacity the organization holds in each region.
 *
 * A region the organization holds nothing in is absent from `allocations` rather than present with a
 * zero, so callers rendering a row per region must default a missing region to zero.
 *
 * Suspense rather than plain `useQuery` is safe here because callers cannot reach this data without
 * already satisfying what the endpoint asks for. The endpoint is `@Secured(ORGANIZATION_ADMIN)`.
 */
export const useListDataWorkerAllocations = () => {
  const requestOptions = useRequestOptions();
  const organizationId = useCurrentOrganizationId();

  return useSuspenseQuery(dataWorkerCapacityKeys.allocationList(organizationId), () =>
    listDataWorkerAllocations({ organization_id: organizationId }, requestOptions)
  );
};

interface ReallocateDataWorkerCapacityVariables {
  fromDataplaneGroupId: string;
  toDataplaneGroupId: string;
  amount: number;
}

/**
 * Moves capacity between two of the organization's regions, leaving the total unchanged — so every
 * increase has to name the region it comes out of.
 *
 * The endpoint answers 400 for an unusable region or a non-positive amount and 409 when the source
 * holds less than the requested amount. Both are plausible from the UI when a concurrent edit has
 * moved capacity since the list was fetched, so the failure is surfaced as a notification rather
 * than left silent.
 */
export const useReallocateDataWorkerCapacity = () => {
  const requestOptions = useRequestOptions();
  const organizationId = useCurrentOrganizationId();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  return useMutation(
    ({ fromDataplaneGroupId, toDataplaneGroupId, amount }: ReallocateDataWorkerCapacityVariables) =>
      reallocateDataWorkerCapacity(
        {
          organization_id: organizationId,
          from_dataplane_group_id: fromDataplaneGroupId,
          to_dataplane_group_id: toDataplaneGroupId,
          amount,
        },
        requestOptions
      ),
    {
      // The endpoint returns the full post-move allocation list, so seeding the cache with it keeps
      // the rendered numbers in step with what the server recorded without a follow-up fetch.
      onSuccess: (response: DataWorkerAllocationListResponse) => {
        queryClient.setQueryData(dataWorkerCapacityKeys.allocationList(organizationId), response);
      },
      onError: () => {
        queryClient.invalidateQueries(dataWorkerCapacityKeys.allocationList(organizationId));
        registerNotification({
          id: "settings.organization.usage.capacity.reallocateError",
          text: formatMessage({ id: "settings.organization.usage.capacity.reallocateError" }),
          type: "error",
        });
      },
    }
  );
};
