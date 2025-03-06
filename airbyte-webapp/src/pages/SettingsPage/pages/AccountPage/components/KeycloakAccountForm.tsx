import React from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";

import { useCurrentUser } from "core/services/auth";

const accountValidationSchema = z.object({
  email: z.string().email("form.email.error"),
});

type KeycloakAccountFormValues = z.infer<typeof accountValidationSchema>;

export const KeycloakAccountForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();

  const onSubmit = async () => {
    Promise.resolve(null);
  };

  return (
    <Form<KeycloakAccountFormValues>
      onSubmit={onSubmit}
      zodSchema={accountValidationSchema}
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
