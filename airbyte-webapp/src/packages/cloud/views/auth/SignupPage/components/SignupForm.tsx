import React, { useMemo } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Form, FormControl } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";

import { AuthSignUp } from "core/services/auth";
import { isGdprCountry } from "core/utils/dataPrivacy";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperiment } from "hooks/services/Experiment";
import { useNotificationService } from "hooks/services/Notification";
import { SignUpFormErrorCodes } from "packages/cloud/services/auth/types";

import { PasswordFormControl } from "../../components/PasswordFormControl";

interface SignupSubmissionButtonProps {
  buttonMessageId: string;
}
export const SignupSubmissionButton: React.FC<SignupSubmissionButtonProps> = ({ buttonMessageId }) => {
  const { isSubmitting } = useFormState();

  return (
    <Button full size="lg" type="submit" isLoading={isSubmitting}>
      <FormattedMessage id={buttonMessageId} />
    </Button>
  );
};

export interface SignupFormValues {
  name?: string;
  companyName?: string;
  email: string;
  password: string;
  news: boolean;
}

export const passwordSchema = yup.string().required("form.empty.error").min(12, "signup.password.minLength");

const composeSignUpSchema = (showName: boolean, showCompanyName: boolean): SchemaOf<SignupFormValues> =>
  yup.object().shape({
    email: yup.string().email("form.email.error").required("form.empty.error"),
    password: passwordSchema,
    news: yup.boolean().required(),
    name: yup.string(),
    companyName: yup.string(),
    ...(showName && { name: yup.string().required("form.empty.error") }),
    ...(showCompanyName && { companyName: yup.string().required("form.empty.error") }),
  });

interface SignupFormProps {
  signUp: AuthSignUp;
}

export const SignupForm: React.FC<SignupFormProps> = ({ signUp }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();

  const showName = !useExperiment("authPage.signup.hideName", false);
  const showCompanyName = !useExperiment("authPage.signup.hideCompanyName", false);

  const validationSchema = useMemo(() => composeSignUpSchema(showName, showCompanyName), [showName, showCompanyName]);

  const [params] = useSearchParams();
  const search = Object.fromEntries(params);

  const defaultValues = {
    name: `${search.firstname ?? ""} ${search.lastname ?? ""}`.trim(),
    companyName: search.company ?? "",
    email: search.email ?? "",
    password: "",
    news: !isGdprCountry(),
  };

  const onSubmit = async (values: SignupFormValues) => {
    await signUp(values);
  };

  const onError = (e: Error, { name, email }: SignupFormValues) => {
    trackError(e, { name, email });

    const errMsg = [
      SignUpFormErrorCodes.EMAIL_INVALID,
      SignUpFormErrorCodes.EMAIL_DUPLICATE,
      SignUpFormErrorCodes.PASSWORD_WEAK,
    ].includes(e.message as SignUpFormErrorCodes)
      ? `signup.${e.message}`
      : "errorView.unknownError";

    registerNotification({
      id: "signup_error",
      text: formatMessage({
        id: errMsg,
      }),
      type: "error",
    });
  };

  return (
    <Form<SignupFormValues>
      defaultValues={defaultValues}
      schema={validationSchema}
      onSubmit={onSubmit}
      onError={onError}
    >
      {(showName || showCompanyName) && (
        <FlexContainer direction="row" justifyContent="space-between" gap="sm">
          {showName && (
            <FlexItem grow>
              <FormControl
                name="name"
                fieldType="input"
                type="text"
                label={formatMessage({ id: "login.fullName" })}
                placeholder={formatMessage({ id: "login.fullName.placeholder" })}
                autoComplete="name"
              />
            </FlexItem>
          )}
          {showCompanyName && (
            <FlexItem grow>
              <FormControl
                name="companyName"
                fieldType="input"
                type="text"
                label={formatMessage({ id: "login.companyName" })}
                placeholder={formatMessage({ id: "login.companyName.placeholder" })}
              />
            </FlexItem>
          )}
        </FlexContainer>
      )}
      <FormControl
        name="email"
        fieldType="input"
        type="text"
        label={formatMessage({ id: "login.yourEmail" })}
        placeholder={formatMessage({ id: "login.yourEmail.placeholder" })}
        autoComplete="email"
        data-testid="signup.email"
      />
      <PasswordFormControl label="login.password" />
      <Box mt="xl">
        <SignupSubmissionButton buttonMessageId="login.signup.submitButton" />
      </Box>
    </Form>
  );
};
