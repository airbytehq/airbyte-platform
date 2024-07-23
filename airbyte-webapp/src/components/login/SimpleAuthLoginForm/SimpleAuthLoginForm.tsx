import React, { useState } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { HttpError } from "core/api";
import { useAuthService } from "core/services/auth";

import styles from "./SimpleAuthLoginForm.module.scss";

export interface SimpleAuthLoginFormValues {
  username: string;
  password: string;
}

const simpleAuthLoginFormSchema = yup.object().shape({
  username: yup.string().required("form.empty.error"),
  password: yup.string().required("form.empty.error"),
});

export const SimpleAuthLoginForm: React.FC = () => {
  const [loginError, setLoginError] = useState<boolean>(false);
  const { login } = useAuthService();

  if (!login) {
    throw new Error("Login function not available");
  }

  return (
    <Form<SimpleAuthLoginFormValues>
      defaultValues={{ username: "", password: "" }}
      schema={simpleAuthLoginFormSchema}
      onSubmit={login}
      onError={(error) => {
        // Indicates incorrect credentials
        if (error instanceof HttpError && error.status === 401) {
          setLoginError(true);
        } else {
          // Otherwise throw in a setState here so that the error is thrown in a render cycle and handled by our error boundary
          setLoginError(() => {
            throw error;
          });
        }
      }}
      reValidateMode="onChange"
    >
      <FormControl fieldType="input" name="username" label="Username" autoComplete="on" />
      <FormControl fieldType="input" name="password" label="Password" autoComplete="on" type="password" />
      <SubmitButton />
      {loginError && (
        <Box mt="2xl">
          <FlexContainer justifyContent="center">
            <Text color="red">
              <FormattedMessage id="login.failed" />
            </Text>
          </FlexContainer>
        </Box>
      )}
    </Form>
  );
};

const SubmitButton = () => {
  const { isValid, isSubmitting } = useFormState();

  return (
    <Box mt="xl">
      <FlexContainer>
        <Button type="submit" isLoading={isSubmitting} disabled={!isValid} className={styles.submitButton}>
          <FormattedMessage id="login.login" />
        </Button>
      </FlexContainer>
    </Box>
  );
};
