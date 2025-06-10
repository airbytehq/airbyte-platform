import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/HeadTitle";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import styles from "./SignupPage.module.scss";
import { Disclaimer } from "../components/Disclaimer";
import { LoginSignupNavigation } from "../components/LoginSignupNavigation";
import { LoginButtons } from "../LoginButtons";

const EmbeddedSignUpPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.EMBEDDED_SIGNUP);

  return (
    <FlexContainer direction="column" gap="xl" className={styles.container}>
      <HeadTitle titles={[{ id: "login.signup" }]} />
      <Text size="xl" as="h1">
        <FormattedMessage id="embedded.signup" />
      </Text>
      <Text size="md">
        <FormattedMessage id="embedded.signup.description" />
      </Text>

      <LoginButtons type="signup" />
      <Disclaimer />
      <LoginSignupNavigation type="login" />
    </FlexContainer>
  );
};

export default EmbeddedSignUpPage;
