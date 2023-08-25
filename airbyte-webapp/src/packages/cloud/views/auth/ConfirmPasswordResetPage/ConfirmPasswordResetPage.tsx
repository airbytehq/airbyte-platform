import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Form, FormControl } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { AuthConfirmPasswordReset } from "core/services/auth";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification/NotificationService";
import { useQuery } from "hooks/useQuery";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { ResetPasswordConfirmErrorCodes } from "packages/cloud/services/auth/types";

interface ResetPasswordConfirmFormValues {
  newPassword: string;
}

const resetPasswordPageValidationSchema: SchemaOf<ResetPasswordConfirmFormValues> = yup.object().shape({
  newPassword: yup.string().required("form.empty.error"),
});

const ResetPasswordButton: React.FC = () => {
  const { isSubmitting } = useFormState();

  return (
    <Button type="submit" data-testid="login.resetPassword" isLoading={isSubmitting}>
      <FormattedMessage id="login.resetPassword" />
    </Button>
  );
};

interface ResetPasswordConfirmPageProps {
  confirmPasswordReset: AuthConfirmPasswordReset;
}

export const ResetPasswordConfirmPage: React.FC<ResetPasswordConfirmPageProps> = ({ confirmPasswordReset }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const navigate = useNavigate();
  const query = useQuery<{ oobCode?: string }>();

  const onSubmit = async ({ newPassword }: ResetPasswordConfirmFormValues) => {
    if (!query.oobCode) {
      return;
    }
    await confirmPasswordReset(query.oobCode, newPassword);
  };

  const onSuccess = () => {
    registerNotification({
      id: "confirmResetPassword.success",
      text: formatMessage({ id: "confirmResetPassword.success" }),
      type: "success",
    });
    navigate(CloudRoutes.Login);
  };

  const onError = (e: Error) => {
    trackError(e);

    const errMsg = [
      ResetPasswordConfirmErrorCodes.LINK_EXPIRED,
      ResetPasswordConfirmErrorCodes.LINK_INVALID,
      ResetPasswordConfirmErrorCodes.PASSWORD_WEAK,
    ].includes(e.message as ResetPasswordConfirmErrorCodes)
      ? `confirmResetPassword.${e.message}`
      : "errorView.unknownError";

    registerNotification({
      id: "confirm_password_reset_error",
      text: formatMessage({
        id: errMsg,
      }),
      type: "error",
    });
  };

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="xl" color="blue">
        <FormattedMessage id="login.resetPassword" />
      </Heading>

      <Form<ResetPasswordConfirmFormValues>
        defaultValues={{
          newPassword: "",
        }}
        schema={resetPasswordPageValidationSchema}
        onSubmit={onSubmit}
        onSuccess={onSuccess}
        onError={onError}
      >
        <FormControl
          name="newPassword"
          fieldType="input"
          type="password"
          label={formatMessage({ id: "confirmResetPassword.newPassword" })}
        />

        <Box mt="2xl">
          <FlexContainer direction="row" justifyContent="space-between" alignItems="center">
            <Text size="sm" color="grey300">
              <Link to={CloudRoutes.Login}>
                <FormattedMessage id="login.backLogin" />
              </Link>
            </Text>
            <ResetPasswordButton />
          </FlexContainer>
        </Box>
      </Form>
    </FlexContainer>
  );
};
