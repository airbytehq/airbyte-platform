import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { AuthChangeName, useCurrentUser } from "core/services/auth";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";

const nameFormSchema = yup.object({
  name: yup.string().required("form.empty.error"),
});

interface NameFormValues {
  name: string;
}

interface NameSectionProps {
  updateName: AuthChangeName;
}

export const NameSection: React.FC<NameSectionProps> = ({ updateName }) => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();
  const { registerNotification } = useNotificationService();

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
        placeholder={formatMessage({
          id: "settings.accountSettings.name.placeholder",
        })}
      />
      <FormSubmissionButtons noCancel justify="flex-start" submitKey="settings.accountSettings.updateName" />
    </Form>
  );
};
