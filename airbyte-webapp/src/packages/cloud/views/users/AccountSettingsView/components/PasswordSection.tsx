import { AuthError as FirebaseAuthError, AuthErrorCodes } from "firebase/auth";
import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Card } from "components/ui/Card";

import { AuthUpdatePassword, useCurrentUser } from "core/services/auth";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification";
import { FirebaseAuthMessageId } from "packages/cloud/services/auth/CloudAuthService";
import { passwordSchema } from "packages/cloud/views/auth/SignupPage/components/SignupForm";

type AuthErrorCodesNames = (typeof AuthErrorCodes)[keyof typeof AuthErrorCodes];

interface AuthError extends FirebaseAuthError {
  /*
   FirebaseError uses {code: string}, so we need to make it typed
   */
  code: AuthErrorCodesNames;
}

// Specific error codes that we want to handle with a custom message
const authErrorCodes: Map<AuthErrorCodesNames, string> = new Map([
  [AuthErrorCodes.INVALID_PASSWORD, FirebaseAuthMessageId.InvalidPassword],
  [AuthErrorCodes.NETWORK_REQUEST_FAILED, FirebaseAuthMessageId.NetworkFailure],
  [AuthErrorCodes.TOO_MANY_ATTEMPTS_TRY_LATER, FirebaseAuthMessageId.TooManyRequests],
]);

const passwordFormSchema = yup.object().shape({
  currentPassword: yup.string().required("form.empty.error"),
  newPassword: passwordSchema.notOneOf(
    [yup.ref("currentPassword")],
    "settings.accountSettings.error.newPasswordSameAsCurrent"
  ),
  passwordConfirmation: yup
    .string()
    .required("form.empty.error")
    .oneOf([yup.ref("newPassword")], "settings.accountSettings.error.newPasswordMismatch"),
});

export interface PasswordFormValues {
  currentPassword: string;
  newPassword: string;
  passwordConfirmation: string;
}

interface PasswordSectionProps {
  updatePassword: AuthUpdatePassword;
}

export const PasswordSection: React.FC<PasswordSectionProps> = ({ updatePassword }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const { email } = useCurrentUser();

  const defaultFormValues: PasswordFormValues = {
    currentPassword: "",
    newPassword: "",
    passwordConfirmation: "",
  };

  const onSuccess = () => {
    registerNotification({
      id: "password_change_success",
      text: formatMessage({ id: "settings.accountSettings.updatePasswordSuccess" }),
      type: "success",
    });
  };

  const onError = (e: Error | AuthError) => {
    trackError(e);

    const defaultUpdatePasswordError = "settings.accountSettings.updatePasswordError";
    let error = defaultUpdatePasswordError;
    if ("code" in e) {
      error = authErrorCodes.get(e.code) || defaultUpdatePasswordError;
    }

    registerNotification({
      id: "password_change_error",
      text:
        error === defaultUpdatePasswordError
          ? formatMessage({ id: error }) + JSON.stringify(e)
          : formatMessage({ id: error }),
      type: "error",
    });
  };

  const onSubmit = async (values: PasswordFormValues) => {
    await updatePassword(email, values.currentPassword, values.newPassword);
    return {
      resetValues: defaultFormValues,
    };
  };

  return (
    <Card withPadding>
      <Form<PasswordFormValues>
        defaultValues={defaultFormValues}
        onSubmit={onSubmit}
        onSuccess={onSuccess}
        onError={onError}
        schema={passwordFormSchema}
      >
        <FormControl<PasswordFormValues>
          label={formatMessage({ id: "settings.accountSettings.currentPassword" })}
          name="currentPassword"
          type="password"
          fieldType="input"
          required
          autoComplete="current-password"
        />
        <FormControl<PasswordFormValues>
          label={formatMessage({ id: "settings.accountSettings.newPassword" })}
          name="newPassword"
          type="password"
          fieldType="input"
          required
          autoComplete="new-password"
        />
        <FormControl<PasswordFormValues>
          label={formatMessage({ id: "settings.accountSettings.newPasswordConfirmation" })}
          name="passwordConfirmation"
          type="password"
          fieldType="input"
          required
        />
        <FormSubmissionButtons submitKey="settings.accountSettings.updatePassword" />
      </Form>
    </Card>
  );
};
