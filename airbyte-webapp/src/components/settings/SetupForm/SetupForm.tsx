import { Field, FieldProps, Form, Formik } from "formik";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import LabeledInput from "components/LabeledInput";
import { LabeledSwitch } from "components/LabeledSwitch";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useConfig } from "config";

import { SecurityCheck } from "./SecurityCheck";

export interface SetupFormValues {
  email: string;
  anonymousDataCollection: boolean;
  news: boolean;
  securityUpdates: boolean;
  securityCheck: "loading" | "secured" | "unsecured" | "check_failed" | "ignored";
}

export interface SetupFormProps {
  onSubmit: (data: SetupFormValues) => void;
}

const SettingHeader: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  return (
    <Box pb="sm">
      <Text size="md" bold>
        {children}
      </Text>
    </Box>
  );
};

const setupFormValidationSchema = yup.object().shape({
  email: yup.string().email("form.email.error").required("form.empty.error"),
  securityCheck: yup.string().oneOf(["succeeded", "ignored", "check_failed"], "securityCheck"),
});

export const SetupForm: React.FC<SetupFormProps> = ({ onSubmit }) => {
  const { formatMessage } = useIntl();
  const config = useConfig();

  return (
    <Formik<SetupFormValues>
      initialValues={{
        email: "",
        anonymousDataCollection: !config.segment.enabled,
        news: false,
        securityUpdates: true,
        securityCheck: "loading",
      }}
      initialErrors={{
        securityCheck: "securityCheck.forbidden",
      }}
      validateOnBlur
      validateOnChange
      validationSchema={setupFormValidationSchema}
      onSubmit={onSubmit}
    >
      {({ isSubmitting, isValid }) => (
        <Form>
          <FlexContainer direction="column" gap="xl">
            <Field name="email">
              {({ field, meta }: FieldProps<string>) => (
                <LabeledInput
                  {...field}
                  label={<FormattedMessage id="form.yourEmail" />}
                  placeholder={formatMessage({
                    id: "form.email.placeholder",
                  })}
                  type="text"
                  error={!!meta.error && meta.touched}
                  message={meta.touched && meta.error && formatMessage({ id: meta.error })}
                />
              )}
            </Field>
            {config.segment.enabled && (
              <Box>
                <SettingHeader>
                  <FormattedMessage id="preferences.anonymizeUsage" />
                </SettingHeader>
                <Text color="grey">
                  <FormattedMessage id="preferences.collectData" />
                </Text>
                <Box mt="sm">
                  <Field name="anonymousDataCollection">
                    {({ field }: FieldProps<string>) => (
                      <LabeledSwitch {...field} label={<FormattedMessage id="preferences.anonymizeData" />} />
                    )}
                  </Field>
                </Box>
              </Box>
            )}
            <Box>
              <SettingHeader>
                <FormattedMessage id="preferences.news" />
              </SettingHeader>
              <Field name="news">
                {({ field }: FieldProps<string>) => (
                  <LabeledSwitch
                    {...field}
                    label={<FormattedMessage id="preferences.featureUpdates" />}
                    message={<FormattedMessage id="preferences.unsubscribeAnyTime" />}
                  />
                )}
              </Field>
            </Box>
            <Box>
              <SettingHeader>
                <FormattedMessage id="preferences.security" />
              </SettingHeader>
              <Field name="securityUpdates">
                {({ field }: FieldProps<string>) => (
                  <LabeledSwitch {...field} label={<FormattedMessage id="preferences.securityUpdates" />} />
                )}
              </Field>
            </Box>
            <Box my="md">
              <SecurityCheck />
            </Box>
            <Text align="center">
              <Button size="lg" type="submit" disabled={!isValid || isSubmitting}>
                <FormattedMessage id="setupForm.submit" />
              </Button>
            </Text>
          </FlexContainer>
        </Form>
      )}
    </Formik>
  );
};
