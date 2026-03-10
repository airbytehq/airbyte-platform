import { NotificationItem } from "core/api/types/AirbyteClient";

import { NotificationSettingsFormValues, notificationKeys } from "./NotificationSettingsForm";
// TODO(https://github.com/airbytehq/hydra-issues-internal/issues/109): When backend API is ready, use NotificationSettings from "core/api/types/AirbyteClient" instead
import { ExtendedNotificationSettings } from "./types";

export function notificationSettingsToFormValues(
  notificationSettings?: ExtendedNotificationSettings
): NotificationSettingsFormValues {
  const formValues: NotificationSettingsFormValues = (
    Object.entries(notificationSettings ?? {}) as Array<[keyof ExtendedNotificationSettings, NotificationItem]>
  ).reduce((acc, [key, value]) => {
    acc[key] = {
      slack: value?.notificationType?.includes("slack") ?? false,
      customerio: value?.notificationType?.includes("customerio") ?? false,
      slackWebhookLink: value?.slackConfiguration?.webhook ?? "",
    };
    return acc;
  }, {} as NotificationSettingsFormValues);

  notificationKeys.forEach((key) => {
    if (!formValues[key]) {
      formValues[key] = {
        slack: false,
        customerio: false,
        slackWebhookLink: "",
      };
    }
  });

  return formValues;
}
