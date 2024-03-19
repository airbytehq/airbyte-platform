import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";

import { useNotificationService } from "hooks/services/Notification";

import { workspaceKeys } from "./workspaces";
import { acceptUserInvitation, createUserInvitation, listPendingInvitations } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION, SCOPE_USER, SCOPE_WORKSPACE } from "../scopes";
import {
  UserInvitationCreateRequestBody,
  UserInvitationListRequestBody,
  UserInvitationRead,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const useAcceptUserInvitation = (inviteCode?: string | null): UserInvitationRead | null => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  return useSuspenseQuery(
    [SCOPE_USER, "userInvitation", inviteCode],
    () =>
      acceptUserInvitation({ inviteCode: inviteCode ?? "" }, requestOptions)
        .then((response) => {
          registerNotification({
            type: "success",
            text: formatMessage({ id: "userInvitations.accept.success" }),
            id: "userInvitations.accept.success",
          });
          queryClient.invalidateQueries(workspaceKeys.lists());
          return response;
        })
        .catch(() => {
          registerNotification({
            type: "error",
            text: formatMessage({ id: "userInvitations.accept.error" }),
            id: "userInvitations.accept.error",
          });
          return null;
        }),
    { enabled: !!inviteCode }
  );
};

export const useCreateUserInvitation = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  return useMutation(async (invitationCreate: UserInvitationCreateRequestBody) =>
    createUserInvitation(invitationCreate, requestOptions)
      .then((response) => {
        registerNotification({
          type: "success",
          text: formatMessage({ id: "userInvitations.create.success" }),
          id: "userInvitations.create.success",
        });
        const keyScope = invitationCreate.scopeType === "workspace" ? SCOPE_WORKSPACE : SCOPE_ORGANIZATION;

        // this endpoint will direct add users who are already within the org, so we want to invalidate both the invitations and the members lists
        queryClient.invalidateQueries(workspaceKeys.allListAccessUsers);
        queryClient.invalidateQueries([keyScope, "userInvitations"]);
        return response;
      })
      .catch(() => {
        registerNotification({
          type: "error",
          text: formatMessage({ id: "userInvitations.create.error" }),
          id: "userInvitations.create.error",
        });
        return null;
      })
  );
};

export const useListUserInvitations = (userInvitationListRequestBody: UserInvitationListRequestBody) => {
  const requestOptions = useRequestOptions();
  const keyScope = userInvitationListRequestBody.scopeType === "workspace" ? SCOPE_WORKSPACE : SCOPE_ORGANIZATION;
  return useSuspenseQuery([keyScope, "userInvitations"], () =>
    listPendingInvitations(userInvitationListRequestBody, requestOptions)
  );
};
