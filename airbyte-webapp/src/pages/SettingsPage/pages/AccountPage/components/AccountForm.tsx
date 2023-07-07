import React, { useEffect } from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace } from "core/api";
import { useNotificationService } from "hooks/services/Notification";
import useWorkspace from "hooks/services/useWorkspace";

const ACCOUNT_UPDATE_NOTIFICATION_ID = "account-update-notification";

const accountValidationSchema = yup.object().shape({
  email: yup.string().email("form.email.error").required("form.empty.error"),
});

interface AccountFormValues {
  email: string;
}

const AccountForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const workspace = useCurrentWorkspace();
  const { updatePreferences } = useWorkspace();

  const onSubmit = async (values: AccountFormValues) => {
    await updatePreferences(values);
  };

  useEffect(
    () => () => {
      unregisterNotificationById(ACCOUNT_UPDATE_NOTIFICATION_ID);
    },
    [unregisterNotificationById]
  );

  return (
    <Form<AccountFormValues>
      onSubmit={onSubmit}
      onSuccess={() => {
        registerNotification({
          id: ACCOUNT_UPDATE_NOTIFICATION_ID,
          text: formatMessage({ id: "form.changesSaved" }),
          type: "success",
        });
      }}
      onError={() => {
        registerNotification({
          id: ACCOUNT_UPDATE_NOTIFICATION_ID,
          text: formatMessage({ id: "form.someError" }),
          type: "error",
        });
      }}
      schema={accountValidationSchema}
      defaultValues={{ email: workspace.email ?? "" }}
    >
      <FormControl<AccountFormValues> label={formatMessage({ id: "form.yourEmail" })} fieldType="input" name="email" />
      <FormSubmissionButtons submitKey="form.saveChanges" />
    </Form>
  );
};

export default AccountForm;
