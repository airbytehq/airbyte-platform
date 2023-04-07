import React from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

import styles from "./LoginSignupNavigation.module.scss";
interface LoginSignupNavigationFlowProps {
  to: "login" | "signup";
}

export const LoginSignupNavigation: React.FC<LoginSignupNavigationFlowProps> = ({ to }) => {
  const navigate = useNavigate();
  return (
    <FlexContainer alignItems="center" justifyContent="center" className={styles.container}>
      <Text>
        <FormattedMessage id={to === "login" ? "login.haveAccount" : "login.noAccount"} />
      </Text>
      <Button variant="secondary" onClick={() => navigate(to === "login" ? CloudRoutes.Login : CloudRoutes.Signup)}>
        <FormattedMessage id={`login.${to}`} />
      </Button>
    </FlexContainer>
  );
};
