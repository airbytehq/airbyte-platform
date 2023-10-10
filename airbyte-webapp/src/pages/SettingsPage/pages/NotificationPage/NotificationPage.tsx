import React from "react";

import { HeadTitle } from "components/common/HeadTitle";
import { NotificationSettingsForm } from "components/NotificationSettingsForm";
import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { WorkspaceEmailForm } from "components/WorkspaceEmailForm";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";

export const NotificationPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_NOTIFICATION);
  const emailNotificationsFeatureEnabled = useFeature(FeatureItem.EmailNotifications);

  return (
    <PageContainer>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.notifications" }]} />
      <FlexContainer direction="column" gap="lg">
        {emailNotificationsFeatureEnabled && (
          <Card>
            <Box p="xl">
              <WorkspaceEmailForm />
            </Box>
          </Card>
        )}
        <Card title="Notification settings">
          <Box p="xl">
            <NotificationSettingsForm />
          </Box>
        </Card>
      </FlexContainer>
    </PageContainer>
  );
};
