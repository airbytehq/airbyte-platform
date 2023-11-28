import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useAuth } from "react-oidc-context";

import { HeadTitle } from "components/common/HeadTitle";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { FeatureItem, useFeature } from "core/services/features";

import { AccountForm } from "./components/AccountForm";
import { KeycloakAccountForm } from "./components/KeycloakAccountForm";

export const AccountPage: React.FC = () => {
  const isKeycloakAuthenticationEnabled = useFeature(FeatureItem.KeycloakAuthentication);

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.account" }]} />
      <Card title={<FormattedMessage id="settings.accountSettings" />}>
        <Box p="xl">{isKeycloakAuthenticationEnabled ? <KeycloakAccountForm /> : <AccountForm />}</Box>
      </Card>
      {isKeycloakAuthenticationEnabled && <SignoutButton />}
    </>
  );
};

const SignoutButton: React.FC = () => {
  const [signoutRedirectPending, setSignnoutRedirectPending] = useState(false);
  const auth = useAuth();

  const handleSignout = () => {
    setSignnoutRedirectPending(true);
    auth.signoutRedirect();
  };
  return (
    <FlexContainer justifyContent="center" alignItems="center">
      <Box p="2xl">
        <Button
          variant="danger"
          onClick={handleSignout}
          isLoading={signoutRedirectPending}
          data-testid="button.signout"
        >
          <FormattedMessage id="settings.accountSettings.logoutText" />
        </Button>
      </Box>
    </FlexContainer>
  );
};
