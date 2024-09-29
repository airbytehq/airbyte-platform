import React, { useState } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useGetInstanceConfiguration } from "core/api";
import { useAuthService } from "core/services/auth";
import { InvalidCredentialsError, MissingCookieError } from "core/services/auth/SimpleAuthService";
import { links } from "core/utils/links";

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
  const [loginError, setLoginError] = useState<null | "missing-cookie" | "invalid-credentials">(null);
  const { login } = useAuthService();
  const { defaultOrganizationEmail } = useGetInstanceConfiguration();

  if (!login) {
    throw new Error("Login function not available");
  }

  return (
    <Form<SimpleAuthLoginFormValues>
      defaultValues={{ username: defaultOrganizationEmail, password: "" }}
      schema={simpleAuthLoginFormSchema}
      onSubmit={login}
      onError={(error) => {
        if (error instanceof InvalidCredentialsError) {
          setLoginError("invalid-credentials");
        } else if (error instanceof MissingCookieError) {
          setLoginError("missing-cookie");
        } else {
          // Otherwise throw in a setState here so that the error is thrown in a render cycle and handled by our error boundary
          setLoginError(() => {
            throw error;
          });
        }
      }}
      reValidateMode="onChange"
    >
      <FormControl fieldType="input" name="username" label="Email" autoComplete="on" type="email" />
      <FormControl fieldType="input" name="password" label="Password" autoComplete="on" type="password" />
      <SubmitButton />
      {loginError && (
        <Box mt="2xl">
          <FlexContainer justifyContent="center">
            <Text color="red" align="center">
              {loginError === "invalid-credentials" && (
                <FormattedMessage
                  id="login.failed"
                  values={{
                    link: (children) => <ExternalLink href={links.ossAuthentication}>{children}</ExternalLink>,
                  }}
                />
              )}
              {loginError === "missing-cookie" && (
                <FormattedMessage
                  id="login.likelyMissingCookie"
                  values={{
                    link: (children) => <ExternalLink href={links.deployingViaHttp}>{children}</ExternalLink>,
                  }}
                />
              )}
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
