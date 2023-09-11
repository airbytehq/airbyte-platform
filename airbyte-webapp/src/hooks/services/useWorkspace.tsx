import { SetupFormValues } from "components/settings/SetupForm/SetupForm";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentWorkspace, useUpdateWorkspace } from "core/api";
import { NotificationSettings } from "core/request/AirbyteClient";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";

export interface WebhookPayload {
  webhook?: string;
  sendOnSuccess?: boolean;
  sendOnFailure?: boolean;
}

const useWorkspace = () => {
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const workspace = useCurrentWorkspace();

  const analyticsService = useAnalyticsService();

  const setInitialSetupConfig = async ({ securityCheck, ...data }: SetupFormValues) => {
    const result = await updateWorkspace({
      workspaceId: workspace.workspaceId,
      initialSetupComplete: true,
      displaySetupWizard: true,
      ...data,
    });

    analyticsService.track(Namespace.ONBOARDING, Action.PREFERENCES, {
      actionDescription: "Setup preferences set",
      email: data.email,
      anonymized: data.anonymousDataCollection,
      security_check_result: securityCheck,
    });

    return result;
  };

  const updatePreferences = async (data: {
    email?: string;
    anonymousDataCollection?: boolean;
    news?: boolean;
    securityUpdates?: boolean;
  }) =>
    await updateWorkspace({
      workspaceId: workspace.workspaceId,
      initialSetupComplete: workspace.initialSetupComplete,
      displaySetupWizard: workspace.displaySetupWizard,
      notifications: workspace.notifications,
      ...data,
    });

  const updateWebhook = async (data: WebhookPayload) => {
    const updatedNotificationSettings: NotificationSettings = {
      sendOnSuccess: data.sendOnSuccess
        ? {
            notificationType: ["slack"],
            slackConfiguration: {
              webhook: data.webhook ?? "",
            },
          }
        : { notificationType: [] },

      sendOnFailure: data.sendOnFailure
        ? {
            notificationType: ["slack"],
            slackConfiguration: {
              webhook: data.webhook ?? "",
            },
          }
        : { notificationType: [] },
      sendOnConnectionUpdate: {
        notificationType: ["customerio"],
      },
      sendOnSyncDisabled: {
        notificationType: ["customerio"],
      },
      sendOnSyncDisabledWarning: {
        notificationType: ["customerio"],
      },
      sendOnConnectionUpdateActionRequired: {
        notificationType: ["customerio"],
      },
      sendOnBreakingChangeWarning: {
        notificationType: ["customerio"],
      },
      sendOnBreakingChangeSyncsDisabled: {
        notificationType: ["customerio"],
      },
    };

    return await updateWorkspace({
      workspaceId: workspace.workspaceId,
      initialSetupComplete: workspace.initialSetupComplete,
      displaySetupWizard: workspace.displaySetupWizard,
      anonymousDataCollection: !!workspace.anonymousDataCollection,
      news: !!workspace.news,
      securityUpdates: !!workspace.securityUpdates,
      notificationSettings: updatedNotificationSettings,
      notifications: [
        {
          notificationType: "slack",
          sendOnSuccess: !!data.sendOnSuccess,
          sendOnFailure: !!data.sendOnFailure,
          slackConfiguration: {
            webhook: data.webhook ?? "",
          },
        },
      ],
    });
  };

  return {
    setInitialSetupConfig,
    updatePreferences,
    updateWebhook,
  };
};

export const useUpdateNotificationSettings = () => {
  const workspaceId = useCurrentWorkspaceId();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();

  return (notificationSettings: NotificationSettings) =>
    updateWorkspace({
      workspaceId,
      notificationSettings,
    });
};

export { useCurrentWorkspace, useWorkspace };
export default useWorkspace;
