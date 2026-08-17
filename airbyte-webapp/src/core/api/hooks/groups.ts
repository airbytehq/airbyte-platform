import { useMutation, useQuery } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import {
  createGroupPermission,
  deleteGroupPermission,
  listGroupMembers,
  listGroupPermissions,
  listGroups,
} from "../generated/AirbyteClient";
import { GroupPermissionCreate, GroupPermissionIdRequestBody } from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export const groupKeys = {
  all: [SCOPE_ORGANIZATION, "groups"] as const,
  lists: () => [...groupKeys.all, "list"] as const,
  list: (organizationId: string) => [...groupKeys.lists(), organizationId] as const,
  memberLists: () => [...groupKeys.all, "members"] as const,
  memberList: (groupId: string) => [...groupKeys.memberLists(), groupId] as const,
  permissionLists: () => [...groupKeys.all, "permissions"] as const,
  permissionList: (groupId: string) => [...groupKeys.permissionLists(), groupId] as const,
};

/**
 * Deliberately a plain `useQuery`. Two reasons, both observed rather than theoretical.
 *
 * The settings shell puts its Suspense boundary inside `SettingsLayoutContent` but has no error
 * boundary, so a suspense read that throws escapes to `DefaultErrorBoundary` and replaces the
 * entire settings layout, sidebar included. `listGroups` is `@Secured(ORGANIZATION_ADMIN)` and
 * additionally demands the `feature-scim` entitlement (`GroupApiController.listGroups`), so a
 * 403 is a reachable state for a real user. It should render inline, on the page.
 *
 * `useSuspenseQuery` also returns `undefined` whenever the query is disabled, while its return type
 * claims otherwise unless `enabled` is the literal `false`. With `enabled: Boolean(organizationId)`
 * the type is wrong at runtime, and destructuring the result crashes.
 */
export const useListGroups = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();

  return useQuery(groupKeys.list(organizationId), () => listGroups({ organizationId }, requestOptions), {
    enabled: Boolean(organizationId),
  });
};

/**
 * Deliberately a plain `useQuery` rather than `useSuspenseQuery`, unlike its sibling read.
 * The only consumer is the per-group "View members" disclosure, which renders inside a
 * `Collapsible`. `Collapsible` passes `unmount={false}` to its `DisclosurePanel`, so panel children
 * stay mounted while collapsed — a suspense read would fire one request per group on page load.
 * `enabled` gates the fetch on expansion instead. A suspense read would also suspend to the
 * settings shell's boundary (blanking the whole content pane on first expand) and throw a member
 * fetch failure up to the app-level error boundary; this way both render inline, on the one card.
 */
export const useListGroupMembers = (groupId: string, options?: { enabled?: boolean }) => {
  const requestOptions = useRequestOptions();

  return useQuery(groupKeys.memberList(groupId), () => listGroupMembers({ groupId }, requestOptions), {
    enabled: options?.enabled ?? true,
  });
};

/**
 * Deliberately a plain `useQuery`, for the same reason as `useListGroups` above: the only caller
 * is the permissions-edit modal, which mounts this read on demand and has no error boundary of its
 * own to catch a suspended throw. `listGroupPermissions` is organization-admin gated plus
 * entitlement gated (`GroupsEntitlementHelper`), so a 403 is a reachable state that must render
 * inline in the modal body rather than escape to the settings shell's boundary.
 *
 * Gated on `enabled` because the modal is the only caller and it mounts on demand.
 */
export const useListGroupPermissions = (groupId: string, options?: { enabled?: boolean }) => {
  const requestOptions = useRequestOptions();

  return useQuery(groupKeys.permissionList(groupId), () => listGroupPermissions({ groupId }, requestOptions), {
    enabled: options?.enabled ?? true,
  });
};

/**
 * No update endpoint exists for group permissions, so every value change saves as a delete plus a
 * create and a single save issues many of these calls at once. The caller owns invalidation: it
 * invalidates `groupKeys.permissionList` once after every operation has settled, rather than once
 * per call. Invalidating here would refetch an active query repeatedly mid-save, and each response
 * would describe a half-applied state.
 *
 * The modal reports failures inline per row, so this hook raises no notification of its own.
 */
export const useCreateGroupPermission = () => {
  const requestOptions = useRequestOptions();

  return useMutation((groupPermission: GroupPermissionCreate) =>
    createGroupPermission(groupPermission, requestOptions)
  );
};

/**
 * The caller owns invalidation. See `useCreateGroupPermission` above.
 */
export const useDeleteGroupPermission = () => {
  const requestOptions = useRequestOptions();

  return useMutation((groupPermissionIdRequestBody: GroupPermissionIdRequestBody) =>
    deleteGroupPermission(groupPermissionIdRequestBody, requestOptions)
  );
};
