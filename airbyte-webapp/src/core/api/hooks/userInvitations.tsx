import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";

import { MessageType } from "components/ui/Message";

import { Notification, useNotificationService } from "hooks/services/Notification";

import { workspaceKeys } from "./workspaces";
import {
  acceptUserInvitation,
  createUserInvitation,
  listPendingInvitations,
  cancelUserInvitation,
} from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION, SCOPE_USER, SCOPE_WORKSPACE } from "../scopes";
import {
  InviteCodeRequestBody,
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
        .catch((err: { response: { message: string }; status?: number }) => {
          const getNotificationFromError = (err: { response: { message: string }; status?: number }): Notification => {
            let notificationId = "userInvitations.accept.error";
            let notificationType: MessageType = "error";

            if (err.status === 403) {
              notificationId = "userInvitations.accept.error.email";
            } else if (err.response.message.endsWith("Status: expired")) {
              notificationId = "userInvitations.accept.error.expired";
            } else if (err.response.message.endsWith("Status: cancelled")) {
              notificationId = "userInvitations.accept.error.cancelled";
            } else if (err.response.message.endsWith("Status: accepted")) {
              notificationId = "userInvitations.accept.warning.alreadyAccepted";
              notificationType = "info";
            }

            return {
              type: notificationType,
              text: formatMessage({ id: notificationId }),
              id: notificationId,
            };
          };
          registerNotification(getNotificationFromError(err));
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
        if (response.directlyAdded === true) {
          registerNotification({
            type: "success",
            text: formatMessage({ id: "userInvitations.create.success.directlyAdded" }),
            id: "userInvitations.create.success.directlyAdded",
          });
          queryClient.invalidateQueries(workspaceKeys.allListAccessUsers);

          return response;
        }
        registerNotification({
          type: "success",
          text: formatMessage({ id: "userInvitations.create.success" }),
          id: "userInvitations.create.success",
        });
        const keyScope = invitationCreate.scopeType === "workspace" ? SCOPE_WORKSPACE : SCOPE_ORGANIZATION;
        queryClient.invalidateQueries([keyScope, "userInvitations"]);

        return response;
      })
      .catch((err) => {
        if (err.status === 409) {
          registerNotification({
            type: "error",
            text: formatMessage({ id: "userInvitations.create.error.duplicate" }),
            id: "userInvitations.create.error.duplicate",
          });
          return null;
        }

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

export const useCancelUserInvitation = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  return useMutation(async (inviteCodeRequestBody: InviteCodeRequestBody) =>
    cancelUserInvitation(inviteCodeRequestBody, requestOptions)
      .then((response) => {
        registerNotification({
          type: "success",
          text: formatMessage({ id: "userInvitations.cancel.success" }),
          id: "userInvitations.cancel.success",
        });

        queryClient.invalidateQueries([
          response.scopeType === "organization" ? SCOPE_ORGANIZATION : SCOPE_WORKSPACE,
          "userInvitations",
        ]);
        return response;
      })
      .catch(() => {
        registerNotification({
          type: "error",
          text: formatMessage({ id: "userInvitations.cancel.error" }),
          id: "userInvitations.cancel.error",
        });
        return null;
      })
  );
};
