import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";

import { useCurrentUser } from "core/services/auth";

const accountValidationSchema = yup.object().shape({
  email: yup.string().email("form.email.error").required("form.empty.error"),
});

interface KeycloakAccountFormValues {
  email: string;
}

export const KeycloakAccountForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();

  const onSubmit = async () => {
    Promise.resolve(null);
  };

  return (
    <Form<KeycloakAccountFormValues>
      onSubmit={onSubmit}
      schema={accountValidationSchema}
      defaultValues={{ email: user.email ?? "" }}
      disabled
    >
      <FormControl<KeycloakAccountFormValues>
        label={formatMessage({ id: "form.yourEmail" })}
        fieldType="input"
        name="email"
      />
    </Form>
  );
};
