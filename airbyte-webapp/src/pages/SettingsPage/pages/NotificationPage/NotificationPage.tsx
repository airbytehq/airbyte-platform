import React from "react";

import { HeadTitle } from "components/common/HeadTitle";
import { NotificationSettingsForm } from "components/NotificationSettingsForm";
import { PageContainer } from "components/PageContainer";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useUpdateNotificationSettings } from "hooks/services/useWorkspace";

export const NotificationPage: React.FC = () => {
  const updateNotificationSettings = useUpdateNotificationSettings();
  useTrackPage(PageTrackingCodes.SETTINGS_NOTIFICATION);

  return (
    <PageContainer>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.notifications" }]} />
      <NotificationSettingsForm updateNotificationSettings={updateNotificationSettings} />
    </PageContainer>
  );
};
