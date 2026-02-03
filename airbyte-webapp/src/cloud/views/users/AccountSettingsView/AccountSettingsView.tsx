import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useAuthService, useCurrentUser } from "core/services/auth";

import { EmailSection, NameSection } from "./components";

export const AccountSettingsView: React.FC = () => {
  const { updateName } = useAuthService();
  const { formatMessage } = useIntl();
  const user = useCurrentUser();

  useTrackPage(PageTrackingCodes.SETTINGS_ACCOUNT);

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer alignItems="center" wrap="wrap">
        <FlexItem grow>
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.accountSettings" />
          </Heading>
        </FlexItem>
        <CopyButton
          content={user.userId}
          variant="clear"
          iconPosition="right"
          title={formatMessage({ id: "settings.userId.copy" })}
        >
          <FormattedMessage id="settings.userId" values={{ id: user.userId }} />
        </CopyButton>
      </FlexContainer>
      <EmailSection />
      {updateName && <NameSection updateName={updateName} />}
    </FlexContainer>
  );
};
