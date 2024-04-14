import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { FeatureItem, useFeature } from "core/services/features";

import { AccountForm } from "./components/AccountForm";
import { KeycloakAccountForm } from "./components/KeycloakAccountForm";

export const AccountPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const isKeycloakAuthenticationEnabled = useFeature(FeatureItem.KeycloakAuthentication);

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1">{formatMessage({ id: "settings.accountSettings" })}</Heading>
      {isKeycloakAuthenticationEnabled ? <KeycloakAccountForm /> : <AccountForm />}
    </FlexContainer>
  );
};
