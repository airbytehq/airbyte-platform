import React from "react";

import { FlexContainer } from "components/ui/Flex";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useAuthService } from "core/services/auth";

import { EmailSection, NameSection, PasswordSection } from "./components";
import { LogoutSection } from "./components/LogoutSection";

export const AccountSettingsView: React.FC = () => {
  const { logout, updateName, hasPasswordLogin, updatePassword } = useAuthService();

  useTrackPage(PageTrackingCodes.SETTINGS_ACCOUNT);

  return (
    <FlexContainer direction="column">
      {updateName && <NameSection updateName={updateName} />}
      <EmailSection />
      {hasPasswordLogin?.() && updatePassword && <PasswordSection updatePassword={updatePassword} />}
      {logout && <LogoutSection logout={logout} />}
    </FlexContainer>
  );
};
