import { useMutation } from "@tanstack/react-query";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { AuthLogout } from "core/services/auth";

export const LogoutSection = ({ logout }: { logout: AuthLogout }) => {
  const { mutateAsync: doLogout, isLoading: isLoggingOut } = useMutation(() => logout());

  return (
    <FlexContainer justifyContent="center" alignItems="center">
      <Box p="2xl">
        <Button variant="danger" onClick={() => doLogout()} isLoading={isLoggingOut} data-testid="button.signout">
          <FormattedMessage id="settings.accountSettings.logoutText" />
        </Button>
      </Box>
    </FlexContainer>
  );
};
