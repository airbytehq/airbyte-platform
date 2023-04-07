import { Field, FieldProps, Formik, Form } from "formik";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";
import styled from "styled-components";
import * as yup from "yup";

import { Button } from "components/ui/Button";

import { useExperiment } from "hooks/services/Experiment";
import { FieldError } from "packages/cloud/lib/errors/FieldError";
import { useAuthService } from "packages/cloud/services/auth/AuthService";
import { isGdprCountry } from "utils/dataPrivacy";

import styles from "./SignupForm.module.scss";
import CheckBoxControl from "../../components/CheckBoxControl";
import { BottomBlock, FieldItem, RowFieldItem } from "../../components/FormComponents";
import { CompanyNameField, EmailField, NameField, PasswordField } from "../../components/FormFields/FormFields";

interface FormValues {
  name: string;
  companyName: string;
  email: string;
  password: string;
  news: boolean;
}

const MarginBlock = styled.div`
  margin-bottom: 15px;
`;

export const NewsField: React.FC = () => (
  <Field name="news">
    {({ field }: FieldProps<string>) => (
      <MarginBlock>
        <CheckBoxControl {...field} checked={!!field.value} label={<FormattedMessage id="login.subscribe" />} />
      </MarginBlock>
    )}
  </Field>
);

interface SignupButtonProps {
  isLoading: boolean;
  disabled: boolean;
  buttonMessageId?: string;
}

export const SignupButton: React.FC<SignupButtonProps> = ({
  isLoading,
  disabled,
  buttonMessageId = "login.signup.submitButton",
}) => (
  <Button full size="lg" type="submit" isLoading={isLoading} disabled={disabled}>
    <FormattedMessage id={buttonMessageId} />
  </Button>
);

export const SignupFormStatusMessage: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <div className={styles.statusMessage}>{children}</div>
);

export const SignupForm: React.FC = () => {
  const { signUp } = useAuthService();

  const showName = !useExperiment("authPage.signup.hideName", false);
  const showCompanyName = !useExperiment("authPage.signup.hideCompanyName", false);

  const validationSchema = useMemo(() => {
    const shape = {
      email: yup.string().email("form.email.error").required("form.empty.error"),
      password: yup.string().min(12, "signup.password.minLength").required("form.empty.error"),
      name: yup.string(),
      companyName: yup.string(),
    };
    if (showName) {
      shape.name = shape.name.required("form.empty.error");
    }
    if (showCompanyName) {
      shape.companyName = shape.companyName.required("form.empty.error");
    }
    return yup.object().shape(shape);
  }, [showName, showCompanyName]);

  const [params] = useSearchParams();
  const search = Object.fromEntries(params);

  const initialValues = {
    name: `${search.firstname ?? ""} ${search.lastname ?? ""}`.trim(),
    companyName: search.company ?? "",
    email: search.email ?? "",
    password: "",
    news: !isGdprCountry(),
  };
  return (
    <Formik<FormValues>
      initialValues={initialValues}
      validationSchema={validationSchema}
      onSubmit={async (values, { setFieldError, setStatus }) =>
        signUp(values).catch((err) => {
          if (err instanceof FieldError) {
            setFieldError(err.field, err.message);
          } else {
            setStatus(err.message);
          }
        })
      }
      validateOnBlur
      validateOnChange
    >
      {({ isValid, isSubmitting, status }) => (
        <Form>
          {(showName || showCompanyName) && (
            <RowFieldItem>
              {showName && <NameField />}
              {showCompanyName && <CompanyNameField />}
            </RowFieldItem>
          )}
          <FieldItem>
            <EmailField />
          </FieldItem>
          <FieldItem>
            <PasswordField />
          </FieldItem>
          <FieldItem>
            <NewsField />
          </FieldItem>
          <BottomBlock>
            <SignupButton isLoading={isSubmitting} disabled={!isValid} />
            {status && <SignupFormStatusMessage>{status}</SignupFormStatusMessage>}
          </BottomBlock>
        </Form>
      )}
    </Formik>
  );
};
