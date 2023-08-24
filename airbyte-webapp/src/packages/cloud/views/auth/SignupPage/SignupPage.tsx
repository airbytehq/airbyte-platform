import { faGoogle } from "@fortawesome/free-brands-svg-icons";
import { faCheckCircle, faEnvelope } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React, { useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate, useSearchParams } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import { trackPageview } from "core/utils/fathom";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

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
      <FontAwesomeIcon icon={faCheckCircle} className={styles.checkIcon} />
      {children}
    </FlexContainer>
  );
};

const SignupPage: React.FC<SignupPageProps> = () => {
  const { loginWithOAuth, signUp } = useAuthService();
  useTrackPage(PageTrackingCodes.SIGNUP);

  useEffect(() => {
    trackPageview();
  }, []);

  const [searchParams] = useSearchParams();

  const navigate = useNavigate();

  const switchSignupMethod = () => {
    navigate(searchParams.get("method") ? CloudRoutes.Signup : `${CloudRoutes.Signup}?method=email`);
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
          <Button onClick={switchSignupMethod} variant="clear" size="sm" icon={<FontAwesomeIcon icon={faGoogle} />}>
            <FormattedMessage id="signup.method.oauth" />
          </Button>
        </>
      ) : (
        <>
          {loginWithOAuth && <OAuthLogin loginWithOAuth={loginWithOAuth} />}
          <Button onClick={switchSignupMethod} variant="clear" size="sm" icon={<FontAwesomeIcon icon={faEnvelope} />}>
            <FormattedMessage id="signup.method.email" />
          </Button>
        </>
      )}

      <Disclaimer />

      <LoginSignupNavigation to="login" />
    </FlexContainer>
  );
};

export default SignupPage;
