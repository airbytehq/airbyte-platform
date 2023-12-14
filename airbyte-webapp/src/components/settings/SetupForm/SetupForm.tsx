import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useSetupInstanceConfiguration } from "core/api";
import { useConfig } from "core/config";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

import { SecurityCheck } from "./SecurityCheck";

type SecurityCheckStatus = "loading" | "check_failed" | "succeeded" | "ignored" | "failed";

export interface SetupFormValues {
  email: string;
  anonymousDataCollection: boolean;
  securityCheck: SecurityCheckStatus;
  organizationName: string;
}

const SubmissionButton: React.FC = () => {
  const { isDirty, isSubmitting, isValid } = useFormState();

  return (
    <Text align="center">
      <Button size="lg" type="submit" disabled={!isDirty || !isValid} isLoading={isSubmitting}>
        <FormattedMessage id="setupForm.submit" />
      </Button>
    </Text>
  );
};

const setupFormValidationSchema = yup.object().shape({
  email: yup.string().email("form.email.error").required("form.empty.error"),
  anonymousDataCollection: yup.bool().required(),
  securityCheck: yup.mixed<SecurityCheckStatus>().oneOf(["succeeded", "ignored", "check_failed"]).required(),
  organizationName: yup.string().required("form.empty.error"),
});

export const SetupForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: setUpInstance } = useSetupInstanceConfiguration();
  const config = useConfig();
  const analyticsService = useAnalyticsService();

  const onSubmit = async (values: SetupFormValues) => {
    await setUpInstance({ ...values, initialSetupComplete: true, displaySetupWizard: false });
    analyticsService.track(Namespace.ONBOARDING, Action.PREFERENCES, {
      actionDescription: "Setup preferences set",
      email: values.email,
      anonymized: values.anonymousDataCollection,
      security_check_result: values.securityCheck,
    });
  };

  return (
    <Form<SetupFormValues>
      defaultValues={{
        email: "",
        anonymousDataCollection: !config.segment.enabled,
        securityCheck: "loading",
      }}
      schema={setupFormValidationSchema}
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
        {config.segment.enabled && (
          <FormControl<SetupFormValues>
            name="anonymousDataCollection"
            fieldType="switch"
            inline
            label={formatMessage({ id: "preferences.anonymizeUsage" })}
            description={formatMessage({ id: "preferences.collectData" })}
          />
        )}
        <Box mb="md">
          <SecurityCheck />
        </Box>
        <SubmissionButton />
      </FlexContainer>
    </Form>
  );
};
