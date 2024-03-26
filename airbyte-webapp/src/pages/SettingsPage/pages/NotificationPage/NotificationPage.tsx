import React from "react";
import { useIntl } from "react-intl";

import { NotificationSettingsForm } from "components/NotificationSettingsForm";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Separator } from "components/ui/Separator";
import { WorkspaceEmailForm } from "components/WorkspaceEmailForm";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";

export const NotificationPage: React.FC = () => {
  const { formatMessage } = useIntl();
  useTrackPage(PageTrackingCodes.SETTINGS_NOTIFICATION);
  const emailNotificationsFeatureEnabled = useFeature(FeatureItem.EmailNotifications);

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1">{formatMessage({ id: "settings.notificationSettings" })}</Heading>
      {emailNotificationsFeatureEnabled && (
        <>
          <WorkspaceEmailForm />
          <Separator />
        </>
      )}
      <NotificationSettingsForm />
    </FlexContainer>
  );
};
