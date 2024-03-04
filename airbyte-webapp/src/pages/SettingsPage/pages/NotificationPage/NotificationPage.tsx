import React from "react";
import { useIntl } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { NotificationSettingsForm } from "components/NotificationSettingsForm";
import { PageContainer } from "components/PageContainer";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { WorkspaceEmailForm } from "components/WorkspaceEmailForm";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";

export const NotificationPage: React.FC = () => {
  const { formatMessage } = useIntl();
  useTrackPage(PageTrackingCodes.SETTINGS_NOTIFICATION);
  const emailNotificationsFeatureEnabled = useFeature(FeatureItem.EmailNotifications);

  return (
    <PageContainer>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.notifications" }]} />
      <FlexContainer direction="column" gap="lg">
        {emailNotificationsFeatureEnabled && (
          <Card>
            <WorkspaceEmailForm />
          </Card>
        )}
        <Card title={formatMessage({ id: "settings.notificationSettings" })} titleWithBottomBorder>
          <NotificationSettingsForm />
        </Card>
      </FlexContainer>
    </PageContainer>
  );
};
