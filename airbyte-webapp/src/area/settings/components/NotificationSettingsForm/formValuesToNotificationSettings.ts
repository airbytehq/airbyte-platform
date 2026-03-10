import { NotificationItem } from "core/api/types/AirbyteClient";

import { NotificationSettingsFormValues, notificationKeys } from "./NotificationSettingsForm";
// TODO(https://github.com/airbytehq/hydra-issues-internal/issues/109): When backend API is ready, use NotificationSettings from "core/api/types/AirbyteClient" instead
import { ExtendedNotificationSettings } from "./types";

export function formValuesToNotificationSettings(
  formValues: NotificationSettingsFormValues
): ExtendedNotificationSettings {
  const notificationSettings: ExtendedNotificationSettings = {};

  notificationKeys.forEach((notificationKey) => {
    const valueFromForm = formValues[notificationKey];
    const notificationItem: NotificationItem = { notificationType: [] };
    if (valueFromForm.customerio) {
      notificationItem.notificationType?.push("customerio");
    }
    if (valueFromForm.slack) {
      notificationItem.notificationType?.push("slack");
    }
    if (valueFromForm.slackWebhookLink) {
      notificationItem.slackConfiguration = {
        webhook: valueFromForm.slackWebhookLink,
      };
    }
    notificationSettings[notificationKey] = notificationItem;
  });

  return notificationSettings;
}
