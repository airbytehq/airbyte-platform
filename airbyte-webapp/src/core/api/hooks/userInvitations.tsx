import { useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";

import { useNotificationService } from "hooks/services/Notification";

import { workspaceKeys } from "./workspaces";
import { acceptUserInvitation } from "../generated/AirbyteClient";
import { SCOPE_USER } from "../scopes";
import { UserInvitationRead } from "../types/AirbyteClient";
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
            timeout: false,
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
