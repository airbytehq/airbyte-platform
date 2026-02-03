import { NotificationItem, NotificationSettings } from "core/api/types/AirbyteClient";

import { NotificationSettingsFormValues, notificationKeys } from "./NotificationSettingsForm";

export function notificationSettingsToFormValues(
  notificationSettings?: NotificationSettings
): NotificationSettingsFormValues {
  const formValues: NotificationSettingsFormValues = (
    Object.entries(notificationSettings ?? {}) as Array<[keyof NotificationSettings, NotificationItem]>
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
