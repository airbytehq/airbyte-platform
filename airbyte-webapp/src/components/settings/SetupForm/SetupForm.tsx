import React from "react";
import { useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useGetInstanceConfiguration, useOssSecurityCheck, useSetupInstanceConfiguration } from "core/api";
import { InstanceConfigurationResponseTrackingStrategy } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useAuthService } from "core/services/auth";

import { SecurityCheck } from "./SecurityCheck";

export const SecurityCheckStatus = {
  loading: "loading",
  check_failed: "check_failed",
  succeeded: "succeeded",
  ignored: "ignored",
  failed: "failed",
  skipped: "skipped",
} as const;

const setupFormValidationSchema = z.object({
  email: z.string().email("form.email.error"),
  anonymousDataCollection: z.boolean(),
  securityCheck: z.nativeEnum(SecurityCheckStatus),
  organizationName: z.string().trim().nonempty("form.empty.error"),
});

export type SetupFormValues = z.infer<typeof setupFormValidationSchema>;

const SubmissionButton: React.FC = () => {
  const { isDirty, isSubmitting, isValid } = useFormState();
  const { isLoading } = useOssSecurityCheck(window.location.origin);
  const securityCheck = useWatch({ name: "securityCheck" });

  return (
    <Text align="center">
      <Button
        size="sm"
        type="submit"
        disabled={
          !isDirty ||
          !isValid ||
          isLoading ||
          securityCheck === SecurityCheckStatus.failed ||
          securityCheck === SecurityCheckStatus.loading
        }
        isLoading={isSubmitting}
      >
        <FormattedMessage id="setupForm.submit" />
      </Button>
    </Text>
  );
};

export const SetupForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const { trackingStrategy } = useGetInstanceConfiguration();
  const { mutateAsync: setUpInstance } = useSetupInstanceConfiguration();
  const analyticsService = useAnalyticsService();
  const { authType } = useAuthService();

  const onSubmit = async (values: SetupFormValues) => {
    await setUpInstance({ ...values, initialSetupComplete: true, displaySetupWizard: false });
    analyticsService.track(Namespace.ONBOARDING, Action.PREFERENCES, {
      actionDescription: "Setup preferences set",
      email: values.email,
      anonymized: values.anonymousDataCollection,
      security_check_result: values.securityCheck,
    });
  };

  // The security check only makes sense for instances with no auth. If auth is enabled, we should just skip it.
  const defaultSecurityCheckValue = authType === "none" ? SecurityCheckStatus.loading : SecurityCheckStatus.skipped;

  return (
    <Form<SetupFormValues>
      defaultValues={{
        email: "",
        anonymousDataCollection: trackingStrategy !== InstanceConfigurationResponseTrackingStrategy.segment,
        securityCheck: defaultSecurityCheckValue,
      }}
      zodSchema={setupFormValidationSchema}
      onSubmit={onSubmit}
    >
      <FlexContainer direction="column">
        <FormControl<SetupFormValues>
          name="email"
          fieldType="input"
          type="text"
          label={formatMessage({ id: "form.yourEmail" })}
          placeholder={formatMessage({ id: "form.email.placeholder" })}
        />
        <FormControl<SetupFormValues>
          name="organizationName"
          fieldType="input"
          type="text"
          label={formatMessage({ id: "form.organizationName" })}
          placeholder={formatMessage({ id: "form.organizationName.placeholder" })}
        />
        {trackingStrategy === InstanceConfigurationResponseTrackingStrategy.segment && (
          <FormControl<SetupFormValues>
            name="anonymousDataCollection"
            fieldType="switch"
            inline
            label={formatMessage({ id: "preferences.anonymizeUsage" })}
            description={formatMessage({ id: "preferences.collectData" })}
          />
        )}
        {defaultSecurityCheckValue !== SecurityCheckStatus.skipped && (
          <Box mb="md">
            <SecurityCheck />
          </Box>
        )}
        <SubmissionButton />
      </FlexContainer>
    </Form>
  );
};
