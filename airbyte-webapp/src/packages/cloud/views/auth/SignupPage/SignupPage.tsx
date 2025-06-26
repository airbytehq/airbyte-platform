import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/HeadTitle";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import styles from "./SignupPage.module.scss";
import { Disclaimer } from "../components/Disclaimer";
import { LoginSignupNavigation } from "../components/LoginSignupNavigation";
import { LoginButtons } from "../LoginButtons";

const Detail: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  return (
    <FlexContainer gap="sm" alignItems="center" className={styles.detailTextContainer}>
      <Icon type="statusSuccess" className={styles.checkIcon} />
      {children}
    </FlexContainer>
  );
};

const SignupPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SIGNUP);

  return (
    <FlexContainer direction="column" gap="xl" justifyContent="center" className={styles.container}>
      <HeadTitle titles={[{ id: "login.signup" }]} />
      <FlexContainer justifyContent="center">
        <Heading as="h1">
          <FormattedMessage id="signup.title" />
        </Heading>
      </FlexContainer>

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
      <LoginButtons type="signup" />
      <Disclaimer />
      <LoginSignupNavigation type="login" />
    </FlexContainer>
  );
};

export default SignupPage;
