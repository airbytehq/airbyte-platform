import React, { useMemo } from "react";

import { HeadTitle } from "components/common/HeadTitle";
import { NotificationSettingsForm } from "components/NotificationSettingsForm";
import { PageContainer } from "components/PageContainer";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useExperiment } from "hooks/services/Experiment";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";

import { WebHookForm } from "./components/WebHookForm";

export const NotificationPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_NOTIFICATION);
  const emailNotificationsExperiment = useExperiment("settings.emailNotifications", false);

  const workspace = useCurrentWorkspace();
  const firstNotification = workspace.notifications?.[0];
  const initialValues = useMemo(
    () => ({
      webhook: firstNotification?.slackConfiguration?.webhook,
      sendOnSuccess: firstNotification?.sendOnSuccess,
      sendOnFailure: firstNotification?.sendOnFailure,
    }),
    [firstNotification]
  );

  return (
    <PageContainer>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.notifications" }]} />
      {!emailNotificationsExperiment && <WebHookForm webhook={initialValues} />}
      {emailNotificationsExperiment && <NotificationSettingsForm />}
    </PageContainer>
  );
};
