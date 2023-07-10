import { faXmarkCircle, faCheckCircle } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import { Field, FieldProps } from "formik";
import React from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { useIntl, FormattedMessage } from "react-intl";

import { LabeledInput } from "components";
import { FormLabel } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

import styles from "./FormFields.module.scss";

/**
 * @deprecated will be removed after migration on react-hook-form
 **/
export const NameField: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <Field name="name">
      {({ field, meta }: FieldProps<string>) => (
        <LabeledInput
          {...field}
          label={<FormattedMessage id="login.fullName" />}
          placeholder={formatMessage({
            id: "login.fullName.placeholder",
          })}
          type="text"
          error={!!meta.error && meta.touched}
          message={meta.touched && meta.error && formatMessage({ id: meta.error })}
        />
      )}
    </Field>
  );
};

/**
 * @deprecated will be removed after migration on react-hook-form
 **/
export const CompanyNameField: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <Field name="companyName">
      {({ field, meta }: FieldProps<string>) => (
        <LabeledInput
          {...field}
          label={<FormattedMessage id="login.companyName" />}
          placeholder={formatMessage({
            id: "login.companyName.placeholder",
          })}
          type="text"
          error={!!meta.error && meta.touched}
          message={meta.touched && meta.error && formatMessage({ id: meta.error })}
        />
      )}
    </Field>
  );
};

/**
 * @deprecated will be removed after migration on react-hook-form
 **/
export const EmailField: React.FC<{ label?: React.ReactNode }> = ({ label }) => {
  const { formatMessage } = useIntl();

  return (
    <Field name="email">
      {({ field, meta }: FieldProps<string>) => (
        <LabeledInput
          {...field}
          label={label || <FormattedMessage id="login.yourEmail" />}
          placeholder={formatMessage({
            id: "login.yourEmail.placeholder",
          })}
          type="text"
          error={Boolean(meta.error) && meta.touched}
          message={meta.touched && meta.error && formatMessage({ id: meta.error })}
          data-testid="input.email"
        />
      )}
    </Field>
  );
};

/**
 * @deprecated Will be replaced by `PasswordFieldHookForm`
 **/
export const PasswordField: React.FC<{ label?: React.ReactNode }> = ({ label }) => {
  const { formatMessage } = useIntl();

  return (
    <Field name="password">
      {({ field, meta }: FieldProps<string>) => (
        <>
          <LabeledInput
            {...field}
            label={label || <FormattedMessage id="login.password" />}
            placeholder={formatMessage({
              id: "login.password.placeholder",
            })}
            type="password"
            error={Boolean(meta.error) && meta.touched}
            data-testid="input.password"
          />

          <FlexContainer gap="sm" alignItems="center" className={styles.passwordCheck__container}>
            <FontAwesomeIcon
              icon={Boolean(meta.error) ? faXmarkCircle : faCheckCircle}
              className={classNames(styles.passwordCheck__icon, {
                [styles["passwordCheck--error"]]: Boolean(meta.error),
                [styles["passwordCheck--valid"]]: !meta.error && field.value,
              })}
            />

            <Text
              size="sm"
              className={classNames({
                [styles["passwordCheck--error"]]: Boolean(meta.error),
              })}
            >
              <FormattedMessage id="signup.password.minLength" />
            </Text>
          </FlexContainer>
        </>
      )}
    </Field>
  );
};

// react-hook-form control
// TODO: rename to "PasswordField" and remove old "PasswordField" after migration to react-hook-form
export const PasswordFieldHookForm: React.FC<{ label?: string }> = ({ label }) => {
  const { formatMessage } = useIntl();
  const {
    errors: { password: error },
    dirtyFields: { password: isDirty },
  } = useFormState({ name: "password" });
  const { register } = useFormContext();

  return (
    <>
      <Box mb="lg">
        <FormLabel label={formatMessage({ id: label ?? "login.password" })} htmlFor="password" />
        <Input
          {...register("password")}
          type="password"
          placeholder={formatMessage({ id: "login.password.placeholder" })}
          error={Boolean(error)}
          autoComplete="new-password"
        />
      </Box>
      <FlexContainer gap="sm" alignItems="center">
        <Icon
          type={error ? "cross" : "check"}
          color={error ? "error" : !isDirty ? "action" : "success"}
          withBackground
          size="sm"
        />
        <Text size="sm" color={error ? "red" : "darkBlue"}>
          <FormattedMessage id="signup.password.minLength" />
        </Text>
      </FlexContainer>
    </>
  );
};

export const Disclaimer: React.FC = () => (
  <Box mt="xl">
    <Text>
      <FormattedMessage
        id="login.disclaimer"
        values={{
          terms: (terms: React.ReactNode) => (
            <ExternalLink href={links.termsLink} variant="primary">
              {terms}
            </ExternalLink>
          ),
          privacy: (privacy: React.ReactNode) => (
            <ExternalLink href={links.privacyLink} variant="primary">
              {privacy}
            </ExternalLink>
          ),
        }}
      />
    </Text>
  </Box>
);
