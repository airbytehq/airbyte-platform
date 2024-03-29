import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Form } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { useUserHook } from "core/api/cloud";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";

import { EmailFormControlList } from "./EmailFormControlList";

export interface InviteUsersFormValues {
  users: Array<{
    role: string;
    email: string;
  }>;
}

const requestConnectorValidationSchema: SchemaOf<InviteUsersFormValues> = yup.object({
  users: yup.array().of(
    yup.object().shape({
      role: yup.string().required("form.empty.error"),
      email: yup.string().required("form.empty.error").email("form.email.error"),
    })
  ),
});
/**
 *
 * @deprecated This component is deprecated and should not be used in new code.
 * @see AddUserModal
 */
export const InviteUsersModal: React.FC<{
  invitedFrom: "source" | "destination" | "user.settings";
  onSubmit: () => void;
  onCancel: () => void;
}> = ({ invitedFrom, onSubmit, onCancel }) => {
  const { formatMessage } = useIntl();
  const { workspaceId } = useCurrentWorkspace();
  const { inviteUserLogic } = useUserHook();
  const { mutateAsync: invite } = inviteUserLogic;

  const { registerNotification } = useNotificationService();
  const analyticsService = useAnalyticsService();

  const onSubmitBtnClick = async (values: InviteUsersFormValues) => {
    await invite({ users: values.users, workspaceId });

    analyticsService.track(Namespace.USER, Action.INVITE, {
      invited_from: invitedFrom,
    });
  };

  const onSuccess = () => {
    registerNotification({
      id: "invite-users-success",
      text: formatMessage({ id: "inviteUsers.invitationsSentSuccess" }),
      type: "success",
    });
    onSubmit();
  };

  const onError = (e: Error, { users }: InviteUsersFormValues) => {
    trackError(e, { users });
    registerNotification({
      id: "invite-users-error",
      text: formatMessage({ id: "inviteUsers.invitationsSentError" }),
      type: "error",
    });
  };

  const formDefaultValues = {
    users: [
      {
        email: "",
        role: "admin", // the only role we have for now
      },
    ],
  };

  return (
    <Form<InviteUsersFormValues>
      schema={requestConnectorValidationSchema}
      defaultValues={formDefaultValues}
      onSubmit={onSubmitBtnClick}
      onSuccess={onSuccess}
      onError={onError}
    >
      <ModalBody>
        <FlexContainer direction="column" gap="none">
          <EmailFormControlList />
        </FlexContainer>
      </ModalBody>
      <ModalFooter>
        <ModalFormSubmissionButtons onCancelClickCallback={onCancel} />
      </ModalFooter>
    </Form>
  );
};
