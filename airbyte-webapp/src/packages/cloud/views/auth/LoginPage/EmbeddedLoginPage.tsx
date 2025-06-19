import React from "react";
import { FormattedMessage } from "react-intl";
import { useLocation } from "react-router-dom";

import { HeadTitle } from "components/HeadTitle";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Message } from "components/ui/Message";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import styles from "./EmbeddedLoginPage.module.scss";
import { Disclaimer } from "../components/Disclaimer";
import { LoginSignupNavigation } from "../components/LoginSignupNavigation";
import { LoginButtons } from "../LoginButtons";

export const EmbeddedLoginPage: React.FC = () => {
  const { state } = useLocation();

  useTrackPage(PageTrackingCodes.LOGIN);

  return (
    <FlexContainer direction="column" gap="xl" className={styles.container}>
      <HeadTitle titles={[{ id: "login.login" }]} />
      <FlexItem>
        <Heading as="h1" size="xl">
          <FormattedMessage id="embedded.loginToAirbyteEmbedded" />
        </Heading>

        {state?.errorMessage && typeof state.errorMessage === "string" && (
          <Box mt="lg">
            <Message type="warning" text={state.errorMessage} />
          </Box>
        )}
      </FlexItem>
      <LoginButtons type="login" />
      <Disclaimer />
      <LoginSignupNavigation type="signup" />
    </FlexContainer>
  );
};

export default EmbeddedLoginPage;
