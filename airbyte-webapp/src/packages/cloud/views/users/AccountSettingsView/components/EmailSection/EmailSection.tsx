import { Field, FieldProps, Form, Formik } from "formik";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import styled from "styled-components";

import { LabeledInput } from "components/LabeledInput";
import { Button } from "components/ui/Button";

import { useCurrentUser } from "packages/cloud/services/auth/AuthService";
import { FieldItem } from "packages/cloud/views/auth/components/FormComponents";
import { Content, SettingsCard } from "pages/SettingsPage/pages/SettingsComponents";

import { useEmail } from "./hooks";
import { FormValues } from "./typings";

const ChangeEmailFooter = styled.div`
  display: flex;
  align-items: center;
  height: 50px;
`;

const TextInputsSection = styled.div`
  display: flex;
  flex-direction: column;
  gap: 15px;
`;

const EmailSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();

  const emailService = useEmail();

  return (
    <SettingsCard>
      <Content>
        <Formik<FormValues>
          initialValues={{
            email: user.email,
            password: "",
          }}
          onSubmit={emailService.updateEmail}
        >
          {({ values }) => (
            <Form>
              <FieldItem>
                <TextInputsSection>
                  <Field name="email">
                    {({ field, meta }: FieldProps<string>) => (
                      <LabeledInput
                        {...field}
                        disabled
                        label={<FormattedMessage id="settings.accountSettings.email" />}
                        placeholder={formatMessage({
                          id: "login.yourEmail.placeholder",
                        })}
                        type="text"
                        error={!!meta.error && meta.touched}
                        message={meta.touched && meta.error && formatMessage({ id: meta.error })}
                      />
                    )}
                  </Field>

                  {user.email !== values.email && (
                    <Field name="password">
                      {({ field, meta }: FieldProps<string>) => (
                        <LabeledInput
                          {...field}
                          label={<FormattedMessage id="settings.accountSettings.enterPassword" />}
                          placeholder=""
                          type="password"
                          error={!!meta.error && meta.touched}
                          message={meta.touched && meta.error && formatMessage({ id: meta.error })}
                        />
                      )}
                    </Field>
                  )}
                </TextInputsSection>
              </FieldItem>
              <ChangeEmailFooter style={{ display: "none" }}>
                <Button type="submit" disabled={user.email === values.email}>
                  <FormattedMessage id="settings.accountSettings.updateEmail" />
                </Button>
              </ChangeEmailFooter>
            </Form>
          )}
        </Formik>
      </Content>
    </SettingsCard>
  );
};

export default EmailSection;
