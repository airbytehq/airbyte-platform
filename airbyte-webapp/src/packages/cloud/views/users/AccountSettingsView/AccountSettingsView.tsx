import React from "react";

import { FlexContainer } from "components/ui/Flex";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useAuthService } from "core/services/auth";

import { EmailSection, NameSection } from "./components";

export const AccountSettingsView: React.FC = () => {
  const { updateName } = useAuthService();

  useTrackPage(PageTrackingCodes.SETTINGS_ACCOUNT);

  return (
    <FlexContainer direction="column" gap="xl">
      <EmailSection />
      {updateName && <NameSection updateName={updateName} />}
    </FlexContainer>
  );
};
