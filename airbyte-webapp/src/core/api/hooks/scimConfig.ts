import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { organizationKeys } from "./organizations";
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
    // Fire-and-forget (not returned), unlike rotate and disable: enable's response carries the
    // one-time token, so mutateAsync must resolve immediately for the credential modal to open
    // with it. The refetches still run; the members page just reaches `scim: true` a tick later.
    onSuccess: () => {
      queryClient.invalidateQueries(scimConfigKeys.detail(organizationId));
      // getOrgInfo is cached under two independent keys; both carry `scim`.
      queryClient.invalidateQueries(organizationKeys.info(organizationId));
      queryClient.invalidateQueries(organizationKeys.orgInfo(organizationId));
    },
  });
};

export const useRotateScimToken = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: () => rotateScimToken({ organizationId }, requestOptions),
    // Returned (not fire-and-forget) so mutateAsync resolves only after the refetch: the
    // confirmation modal stays in its loading state until the refetch settles, then the
    // one-time credential modal opens against a consistent cache.
    onSuccess: () =>
      Promise.all([
        queryClient.invalidateQueries(scimConfigKeys.detail(organizationId)),
        queryClient.invalidateQueries(organizationKeys.info(organizationId)),
        queryClient.invalidateQueries(organizationKeys.orgInfo(organizationId)),
      ]),
  });
};

export const useDisableScim = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: () => disableScim({ organizationId }, requestOptions),
    // Returned (not fire-and-forget) so mutateAsync resolves only after the refetch: the
    // confirmation modal then closes exactly when the card flips to disabled, with no stale window.
    onSuccess: () =>
      Promise.all([
        queryClient.invalidateQueries(scimConfigKeys.detail(organizationId)),
        queryClient.invalidateQueries(organizationKeys.info(organizationId)),
        queryClient.invalidateQueries(organizationKeys.orgInfo(organizationId)),
      ]),
  });
};
