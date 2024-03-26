import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Separator } from "components/ui/Separator";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useAuthService } from "core/services/auth";

import { EmailSection, NameSection, PasswordSection } from "./components";

export const AccountSettingsView: React.FC = () => {
  const { updateName, hasPasswordLogin, updatePassword } = useAuthService();

  useTrackPage(PageTrackingCodes.SETTINGS_ACCOUNT);

  return (
    <FlexContainer direction="column" gap="xl">
      <EmailSection />
      {updateName && <NameSection updateName={updateName} />}
      {hasPasswordLogin?.() && updatePassword && (
        <>
          <Separator />
          <PasswordSection updatePassword={updatePassword} />
        </>
      )}
    </FlexContainer>
  );
};
