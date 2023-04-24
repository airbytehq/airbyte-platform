import { faXmarkCircle, faCheckCircle } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import { Field, FieldProps } from "formik";
import { useIntl, FormattedMessage } from "react-intl";

import { LabeledInput } from "components";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "utils/links";

import styles from "./FormFields.module.scss";

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

export const Disclaimer: React.FC = () => {
  return (
    <Text className={styles.disclaimer}>
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
  );
};
