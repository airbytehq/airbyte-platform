import React from "react";
import { FormattedMessage } from "react-intl";
import { createSearchParams, useNavigate, useSearchParams } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

import styles from "./LoginSignupNavigation.module.scss";
interface LoginSignupNavigationFlowProps {
  type: "login" | "signup";
}

export const LoginSignupNavigation: React.FC<LoginSignupNavigationFlowProps> = ({ type }) => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const loginRedirectString = searchParams.get("loginRedirect");
  const reStringifiedLoginRedirect = loginRedirectString && createSearchParams({ loginRedirect: loginRedirectString });

  const loginTo = loginRedirectString
    ? { pathname: CloudRoutes.Login, search: `${reStringifiedLoginRedirect}` }
    : CloudRoutes.Login;
  const signupTo = loginRedirectString
    ? { pathname: CloudRoutes.Signup, search: `${reStringifiedLoginRedirect}` }
    : CloudRoutes.Signup;

  return (
    <FlexContainer alignItems="center" justifyContent="center" className={styles.container}>
      <Text>
        <FormattedMessage id={type === "login" ? "login.haveAccount" : "login.noAccount"} />
      </Text>
      <Button variant="secondary" onClick={() => navigate(type === "login" ? loginTo : signupTo)}>
        <FormattedMessage id={`login.${type}`} />
      </Button>
    </FlexContainer>
  );
};
