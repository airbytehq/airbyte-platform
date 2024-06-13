import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";

import { useCurrentUser } from "core/services/auth";

import styles from "./EmailSection.module.scss";

const emailFormSchema = yup.object({
  email: yup.string().required("form.empty.error"),
});

export interface EmailFormValues {
  email: string;
}

export const EmailSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();

  return (
    <Form<EmailFormValues>
      defaultValues={{
        email: user.email,
      }}
      schema={emailFormSchema}
    >
      <FormControl<EmailFormValues>
        containerControlClassName={styles.emailControl}
        name="email"
        fieldType="input"
        type="text"
        label={formatMessage({ id: "settings.accountSettings.email" })}
        placeholder={formatMessage({
          id: "login.yourEmail.placeholder",
        })}
        /*
            show user's email in read-only mode, details: https://github.com/airbytehq/airbyte-platform-internal/issues/1269
           */
        disabled
      />
    </Form>
  );
};
