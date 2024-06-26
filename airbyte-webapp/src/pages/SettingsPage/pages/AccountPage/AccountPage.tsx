import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useAuthService } from "core/services/auth";

import { AccountForm } from "./components/AccountForm";
import { KeycloakAccountForm } from "./components/KeycloakAccountForm";

export const AccountPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const { authType } = useAuthService();

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1">{formatMessage({ id: "settings.accountSettings" })}</Heading>
      {authType === "oidc" ? <KeycloakAccountForm /> : <AccountForm />}
    </FlexContainer>
  );
};
