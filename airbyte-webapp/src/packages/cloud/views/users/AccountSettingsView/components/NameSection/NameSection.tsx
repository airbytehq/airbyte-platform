import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Card } from "components/ui/Card";

import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification";
import { useAuthService, useCurrentUser } from "packages/cloud/services/auth/AuthService";

const nameFormSchema = yup.object({
  name: yup.string().required("form.empty.error"),
});

interface NameFormValues {
  name: string;
}

export const NameSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();
  const { updateName } = useAuthService();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();

  const onSuccess = () => {
    registerNotification({
      id: "name_change_success",
      text: formatMessage({ id: "settings.accountSettings.updateNameSuccess" }),
      type: "success",
    });
  };

  const onError = (e: Error, { name }: NameFormValues) => {
    trackError(e, { name });
    registerNotification({
      id: "name_change_error",
      text: formatMessage({ id: "settings.accountSettings.updateNameError" }),
      type: "error",
    });
  };

  return (
    <Card withPadding>
      <Form<NameFormValues>
        onSubmit={({ name }) => updateName(name)}
        onError={onError}
        onSuccess={onSuccess}
        schema={nameFormSchema}
        defaultValues={{ name: user.name }}
      >
        <FormControl<NameFormValues>
          label={formatMessage({ id: "settings.accountSettings.name" })}
          fieldType="input"
          name="name"
        />
        <FormSubmissionButtons submitKey="settings.accountSettings.updateName" />
      </Form>
    </Card>
  );
};
