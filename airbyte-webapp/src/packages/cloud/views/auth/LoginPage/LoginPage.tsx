import React from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import styles from "./LoginPage.module.scss";
import { Disclaimer } from "../components/Disclaimer";
import { LoginSignupNavigation } from "../components/LoginSignupNavigation";
import { LoginButtons } from "../LoginButtons";

export const LoginPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const loginRedirectString = searchParams.get("loginRedirect");
  const isAcceptingInvitation = loginRedirectString?.includes("accept-invite");

  useTrackPage(PageTrackingCodes.LOGIN);

  return (
    <FlexContainer direction="column" gap="xl" className={styles.container}>
      <HeadTitle titles={[{ id: "login.login" }]} />
      <FlexItem>
        <Heading as="h1" size="xl" color="blue">
          <FormattedMessage id={isAcceptingInvitation ? "login.acceptInvite" : "login.loginTitle"} />
        </Heading>
        {isAcceptingInvitation && (
          <Box pt="md">
            <Heading as="h2" size="md" color="darkBlue">
              <FormattedMessage id="login.acceptInvite.subtitle" />
            </Heading>
          </Box>
        )}
      </FlexItem>
      <LoginButtons type="login" />
      <Disclaimer />
      <LoginSignupNavigation type="signup" />
    </FlexContainer>
  );
};
