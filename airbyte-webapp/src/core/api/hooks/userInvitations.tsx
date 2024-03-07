import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";

import { useNotificationService } from "hooks/services/Notification";

import { workspaceKeys } from "./workspaces";
import { acceptUserInvitation, createUserInvitation } from "../generated/AirbyteClient";
import { SCOPE_USER } from "../scopes";
import { UserInvitationCreateRequestBody, UserInvitationRead } from "../types/AirbyteClient";
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
        queryClient.invalidateQueries(workspaceKeys.allListAccessUsers);
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
