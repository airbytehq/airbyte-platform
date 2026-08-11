import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useNotificationService } from "core/services/Notification";

import { HttpError, HttpProblem } from "../errors";
import {
  addGroupMember,
  createGroup,
  deleteGroup,
  getGroup,
  listGroupMembers,
  listGroups,
  removeGroupMember,
  updateGroup,
} from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { GroupCreate, GroupMemberRequestBody, GroupUpdate } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const groupKeys = {
  all: [SCOPE_ORGANIZATION, "groups"] as const,
  lists: () => [...groupKeys.all, "list"] as const,
  list: (organizationId: string) => [...groupKeys.lists(), organizationId] as const,
  detail: (groupId: string) => [...groupKeys.all, "detail", groupId] as const,
  memberLists: () => [...groupKeys.all, "members"] as const,
  memberList: (groupId: string) => [...groupKeys.memberLists(), groupId] as const,
};

/**
 * Extracts the server-provided message for the problem types a group endpoint can throw (see
 * GroupApiController.kt). Five of them (bad-request, already-exists, managed-by-scim,
 * member-already-exists, state-conflict) carry the message in `data.message`
 * (`ProblemMessageData`). `ResourceNotFoundProblem` is the exception: its `data` is
 * `ProblemResourceData` (`resourceType` / `resourceId`, no `message`), so
 * `GroupApiController.getAuthorizedGroup()` carries the human-readable text via the constructor's
 * first argument instead, which lands in `detail`.
 *
 * Which endpoint throws what: create -> bad-request, already-exists; update -> bad-request,
 * already-exists, managed-by-scim; delete -> managed-by-scim; add member ->
 * member-already-exists, state-conflict (inactive user), resource-not-found (user not in the
 * organization), managed-by-scim; remove member -> managed-by-scim. Every group-scoped operation
 * can additionally throw resource-not-found for an unknown group.
 */
function getGroupProblemMessage(error: unknown): string | undefined {
  if (!(error instanceof HttpError)) {
    return undefined;
  }
  if (HttpProblem.isType(error, "https://reference.airbyte.com/reference/errors#resource-not-found")) {
    return error.response.detail;
  }
  if (
    HttpProblem.isType(error, "error:group-already-exists") ||
    HttpProblem.isType(error, "error:group-managed-by-scim") ||
    HttpProblem.isType(error, "error:group-member-already-exists") ||
    HttpProblem.isType(error, "https://reference.airbyte.com/reference/errors#409-state-conflict") ||
    HttpProblem.isType(error, "https://reference.airbyte.com/reference/errors#bad-request")
  ) {
    return error.response.data?.message;
  }
  return undefined;
}

export const useListGroups = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(groupKeys.list(organizationId), () => listGroups({ organizationId }, requestOptions), {
    enabled: Boolean(organizationId),
  });
};

export const useGetGroup = (groupId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(groupKeys.detail(groupId), () => getGroup({ groupId }, requestOptions));
};

export const useListGroupMembers = (groupId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(groupKeys.memberList(groupId), () => listGroupMembers({ groupId }, requestOptions));
};

export const useCreateGroup = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  return useMutation(
    (group: Omit<GroupCreate, "organizationId">) => createGroup({ ...group, organizationId }, requestOptions),
    {
      onSuccess: () => {
        registerNotification({
          id: "settings.organization.groups.create.success",
          text: formatMessage({ id: "settings.organization.groups.create.success" }),
          type: "success",
        });
        queryClient.invalidateQueries(groupKeys.list(organizationId));
      },
      onError: (error) => {
        registerNotification({
          id: "settings.organization.groups.create.error",
          text: getGroupProblemMessage(error) ?? formatMessage({ id: "settings.organization.groups.create.error" }),
          type: "error",
        });
      },
    }
  );
};

export const useUpdateGroup = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  return useMutation((group: GroupUpdate) => updateGroup(group, requestOptions), {
    onSuccess: (data) => {
      registerNotification({
        id: "settings.organization.groups.update.success",
        text: formatMessage({ id: "settings.organization.groups.update.success" }),
        type: "success",
      });
      queryClient.invalidateQueries(groupKeys.list(organizationId));
      queryClient.invalidateQueries(groupKeys.detail(data.groupId));
    },
    onError: (error) => {
      registerNotification({
        id: "settings.organization.groups.update.error",
        text: getGroupProblemMessage(error) ?? formatMessage({ id: "settings.organization.groups.update.error" }),
        type: "error",
      });
    },
  });
};

export const useDeleteGroup = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  return useMutation((groupId: string) => deleteGroup({ groupId }, requestOptions), {
    onSuccess: (_data, groupId) => {
      registerNotification({
        id: "settings.organization.groups.delete.success",
        text: formatMessage({ id: "settings.organization.groups.delete.success" }),
        type: "success",
      });
      queryClient.invalidateQueries(groupKeys.list(organizationId));
      queryClient.invalidateQueries(groupKeys.detail(groupId));
    },
    onError: (error) => {
      registerNotification({
        id: "settings.organization.groups.delete.error",
        text: getGroupProblemMessage(error) ?? formatMessage({ id: "settings.organization.groups.delete.error" }),
        type: "error",
      });
    },
  });
};

export const useAddGroupMember = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  return useMutation((member: GroupMemberRequestBody) => addGroupMember(member, requestOptions), {
    onSuccess: (_data, variables) => {
      registerNotification({
        id: "settings.organization.groups.addMember.success",
        text: formatMessage({ id: "settings.organization.groups.addMember.success" }),
        type: "success",
      });
      queryClient.invalidateQueries(groupKeys.memberList(variables.groupId));
      // GroupRead carries memberCount, so the group list is stale too once membership changes.
      queryClient.invalidateQueries(groupKeys.list(organizationId));
    },
    onError: (error) => {
      registerNotification({
        id: "settings.organization.groups.addMember.error",
        text: getGroupProblemMessage(error) ?? formatMessage({ id: "settings.organization.groups.addMember.error" }),
        type: "error",
      });
    },
  });
};

export const useRemoveGroupMember = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  return useMutation((member: GroupMemberRequestBody) => removeGroupMember(member, requestOptions), {
    onSuccess: (_data, variables) => {
      registerNotification({
        id: "settings.organization.groups.removeMember.success",
        text: formatMessage({ id: "settings.organization.groups.removeMember.success" }),
        type: "success",
      });
      queryClient.invalidateQueries(groupKeys.memberList(variables.groupId));
      // GroupRead carries memberCount, so the group list is stale too once membership changes.
      queryClient.invalidateQueries(groupKeys.list(organizationId));
    },
    onError: (error) => {
      registerNotification({
        id: "settings.organization.groups.removeMember.error",
        text: getGroupProblemMessage(error) ?? formatMessage({ id: "settings.organization.groups.removeMember.error" }),
        type: "error",
      });
    },
  });
};
