import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { HeadTitle } from "components/common/HeadTitle";
import { Form, FormControl } from "components/forms";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";

import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification";
import { useAuthService } from "packages/cloud/services/auth/AuthService";
import { EmailLinkErrorCodes } from "packages/cloud/services/auth/types";
import { isGdprCountry } from "utils/dataPrivacy";

import { Disclaimer, PasswordFieldHookForm } from "./auth/components/FormFields/FormFields";
import { FormTitle } from "./auth/components/FormTitle";
import { passwordSchema, SignupButtonHookForm } from "./auth/SignupPage/components/SignupForm";

interface AcceptEmailInviteFormValues {
  name: string;
  email: string;
  password: string;
  news: boolean;
}

const acceptEmailInviteSchema: SchemaOf<AcceptEmailInviteFormValues> = yup.object().shape({
  name: yup.string().required("form.empty.error"),
  email: yup.string().email("form.email.error").required("form.empty.error"),
  password: passwordSchema,
  news: yup.boolean().required(),
});

export const AcceptEmailInvite: React.FC = () => {
  const { formatMessage } = useIntl();
  const authService = useAuthService();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();

  const onSubmit = async (values: AcceptEmailInviteFormValues) => {
    await authService.signUpWithEmailLink(values);
  };

  const onError = (e: Error, { name, email }: AcceptEmailInviteFormValues) => {
    trackError(e, { name, email });

    const errMsg = [
      EmailLinkErrorCodes.LINK_EXPIRED,
      EmailLinkErrorCodes.LINK_INVALID,
      EmailLinkErrorCodes.EMAIL_MISMATCH,
    ].includes(e.message as EmailLinkErrorCodes)
      ? `login.${e.message}`
      : "errorView.unknownError";

    registerNotification({
      id: "accept_email_invite_error",
      text: formatMessage({
        id: errMsg,
      }),
      type: "error",
    });
  };

  return (
    <>
      <HeadTitle titles={[{ id: "login.inviteTitle" }]} />
      <FormTitle>
        <FormattedMessage id="login.inviteTitle" />
      </FormTitle>
      <Form
        defaultValues={{
          name: "",
          email: "",
          password: "",
          news: !isGdprCountry(),
        }}
        schema={acceptEmailInviteSchema}
        onSubmit={onSubmit}
        onError={onError}
      >
        <FlexContainer direction="column" gap="none">
          <FormControl
            name="name"
            fieldType="input"
            type="text"
            label={formatMessage({ id: "login.fullName" })}
            placeholder={formatMessage({ id: "login.fullName.placeholder" })}
            autoComplete="name"
          />
          <FormControl
            name="email"
            fieldType="input"
            type="text"
            label={formatMessage({ id: "login.inviteEmail" })}
            placeholder={formatMessage({ id: "login.yourEmail.placeholder" })}
            autoComplete="email"
          />
          <PasswordFieldHookForm label="login.createPassword" />
          <Box mt="xl">
            <SignupButtonHookForm buttonMessageId="login.activateAccess.submitButton" />
          </Box>
          <Disclaimer />
        </FlexContainer>
      </Form>
    </>
  );
};

export default AcceptEmailInvite;
