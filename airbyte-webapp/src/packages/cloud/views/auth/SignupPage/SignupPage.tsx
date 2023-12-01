import React from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useAuthService } from "core/services/auth";

import { SignupForm } from "./components/SignupForm";
import styles from "./SignupPage.module.scss";
import { Disclaimer } from "../components/Disclaimer";
import { LoginSignupNavigation } from "../components/LoginSignupNavigation";
import { OAuthLogin } from "../OAuthLogin";

interface SignupPageProps {
  highlightStyle?: React.CSSProperties;
}
const Detail: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  return (
    <FlexContainer gap="sm" alignItems="center" className={styles.detailTextContainer}>
      <Icon type="statusSuccess" className={styles.checkIcon} />
      {children}
    </FlexContainer>
  );
};

const SignupPage: React.FC<SignupPageProps> = () => {
  const { loginWithOAuth, signUp } = useAuthService();
  useTrackPage(PageTrackingCodes.SIGNUP);

  const [searchParams, setSearchParams] = useSearchParams();

  const setSignupMethod = (method?: "email") => {
    if (method) {
      searchParams.set("method", method);
    } else {
      searchParams.delete("method");
    }
    setSearchParams(searchParams);
  };

  return (
    <FlexContainer direction="column" gap="xl" justifyContent="center" className={styles.container}>
      <HeadTitle titles={[{ id: "login.signup" }]} />
      <Heading as="h1" centered>
        <FormattedMessage id="signup.title" />
      </Heading>

      <FlexContainer direction="column" justifyContent="center" alignItems="flex-start" className={styles.details}>
        <Detail>
          <FormattedMessage id="signup.details.noCreditCard" />
        </Detail>
        <Detail>
          <FormattedMessage id="signup.details.instantSetup" />
        </Detail>
        <Detail>
          <FormattedMessage id="signup.details.freeTrial" />
        </Detail>
      </FlexContainer>
      {searchParams.get("method") === "email" ? (
        <>
          {signUp && <SignupForm signUp={signUp} />}
          <Button onClick={() => setSignupMethod()} variant="clear" size="sm" icon={<Icon type="google" />}>
            <FormattedMessage id="signup.method.oauth" />
          </Button>
        </>
      ) : (
        <>
          {loginWithOAuth && <OAuthLogin loginWithOAuth={loginWithOAuth} />}
          <Button onClick={() => setSignupMethod("email")} variant="clear" size="sm" icon={<Icon type="ticket" />}>
            <FormattedMessage id="signup.method.email" />
          </Button>
        </>
      )}

      <Disclaimer />

      <LoginSignupNavigation type="login" />
    </FlexContainer>
  );
};

export default SignupPage;
