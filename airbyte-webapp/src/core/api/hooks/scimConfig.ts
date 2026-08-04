import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { disableScim, enableScim, getScimConfig, rotateScimToken } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { ScimIdpProvider } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const scimConfigKeys = {
  all: [SCOPE_ORGANIZATION, "scimConfig"] as const,
  detail: (organizationId: string) => [...scimConfigKeys.all, organizationId] as const,
};

/**
 * Callers outside `useScimSettingsAccess` (in `area/organization/utils`) should consume that hook
 * instead — its `canManageScim` is the only sanctioned value for `enabled` (the config read is
 * `ORGANIZATION_ADMIN`-secured server-side).
 */
export const useGetScimConfig = (options: { enabled: boolean }) => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();

  return useQuery(scimConfigKeys.detail(organizationId), () => getScimConfig({ organizationId }, requestOptions), {
    enabled: !!organizationId && options.enabled,
  });
};

export const useEnableScim = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (idpProvider: ScimIdpProvider) => enableScim({ organizationId, idpProvider }, requestOptions),
    onSuccess: () => {
      queryClient.invalidateQueries(scimConfigKeys.detail(organizationId));
    },
  });
};

export const useRotateScimToken = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: () => rotateScimToken({ organizationId }, requestOptions),
    onSuccess: () => {
      queryClient.invalidateQueries(scimConfigKeys.detail(organizationId));
    },
  });
};

export const useDisableScim = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: () => disableScim({ organizationId }, requestOptions),
    onSuccess: () => {
      queryClient.invalidateQueries(scimConfigKeys.detail(organizationId));
    },
  });
};
