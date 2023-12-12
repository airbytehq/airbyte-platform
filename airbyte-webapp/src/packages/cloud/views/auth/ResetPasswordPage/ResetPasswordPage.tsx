import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { createSearchParams, useSearchParams } from "react-router-dom";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { HeadTitle } from "components/common/HeadTitle";
import { Form, FormControl } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { AuthRequirePasswordReset } from "core/services/auth";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification/NotificationService";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

import { LoginSignupNavigation } from "../components/LoginSignupNavigation";

interface ResetPasswordFormValues {
  email: string;
}

const resetPasswordPageValidationSchema: SchemaOf<ResetPasswordFormValues> = yup.object().shape({
  email: yup.string().email("form.email.error").required("form.empty.error"),
});

const ResetPasswordButton: React.FC = () => {
  const { isSubmitting } = useFormState();

  return (
    <Button type="submit" data-testid="login.resetPassword" isLoading={isSubmitting}>
      <FormattedMessage id="login.resetPassword" />
    </Button>
  );
};

interface ResetPasswordPageProps {
  requirePasswordReset: AuthRequirePasswordReset;
}

export const ResetPasswordPage: React.FC<ResetPasswordPageProps> = ({ requirePasswordReset }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const [searchParams] = useSearchParams();

  const loginRedirectString = searchParams.get("loginRedirect");

  const reStringifiedLoginRedirect = loginRedirectString && createSearchParams({ loginRedirect: loginRedirectString });
  const loginTo = loginRedirectString
    ? { pathname: CloudRoutes.Login, search: `${reStringifiedLoginRedirect}` }
    : CloudRoutes.Login;

  useTrackPage(PageTrackingCodes.RESET_PASSWORD);

  const onSubmit = async ({ email }: ResetPasswordFormValues) => {
    await requirePasswordReset(email);

    registerNotification({
      id: "resetPassword.emailSent",
      text: formatMessage({ id: "login.resetPassword.emailSent" }),
      type: "success",
    });
  };

  const onError = (e: Error, { email }: ResetPasswordFormValues) => {
    trackError(e, { email });

    registerNotification({
      id: "reset_password_error",
      text: formatMessage({
        id: e.message.includes("user-not-found") ? "login.yourEmail.notFound" : "login.unknownError",
      }),
      type: "error",
    });
  };

  return (
    <FlexContainer direction="column" gap="xl">
      <HeadTitle titles={[{ id: "login.resetPassword" }]} />

      <Heading as="h1" size="xl" color="blue">
        <FormattedMessage id="login.resetPassword" />
      </Heading>

      <Form<ResetPasswordFormValues>
        defaultValues={{
          email: "",
        }}
        schema={resetPasswordPageValidationSchema}
        onSubmit={onSubmit}
        onError={onError}
      >
        <FormControl
          name="email"
          fieldType="input"
          type="text"
          label={formatMessage({ id: "login.yourEmail" })}
          placeholder={formatMessage({ id: "login.yourEmail.placeholder" })}
        />

        <Box mt="2xl">
          <FlexContainer direction="row" justifyContent="space-between" alignItems="center">
            <Text size="sm" color="grey300">
              <Link to={loginTo}>
                <FormattedMessage id="login.backLogin" />
              </Link>
            </Text>
            <ResetPasswordButton />
          </FlexContainer>
        </Box>
      </Form>
      <LoginSignupNavigation type="signup" />
    </FlexContainer>
  );
};
