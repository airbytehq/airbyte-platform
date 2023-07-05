import { useMutation } from "@tanstack/react-query";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useAuthService } from "packages/cloud/services/auth/AuthService";

import { EmailSection, NameSection, PasswordSection } from "./components";

export const AccountSettingsView: React.FC = () => {
  const authService = useAuthService();
  const { mutateAsync: logout, isLoading: isLoggingOut } = useMutation(() => authService.logout());

  useTrackPage(PageTrackingCodes.SETTINGS_ACCOUNT);

  return (
    <FlexContainer direction="column">
      <NameSection />
      <EmailSection />
      <PasswordSection />
      <FlexContainer justifyContent="center" alignItems="center">
        <Box p="2xl">
          <Button variant="danger" onClick={() => logout()} isLoading={isLoggingOut} data-testid="button.signout">
            <FormattedMessage id="settings.accountSettings.logoutText" />
          </Button>
        </Box>
      </FlexContainer>
    </FlexContainer>
  );
};
