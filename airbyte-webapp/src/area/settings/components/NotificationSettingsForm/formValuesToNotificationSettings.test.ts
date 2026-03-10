import { NotificationItem } from "core/api/types/AirbyteClient";

import { formValuesToNotificationSettings } from "./formValuesToNotificationSettings";
import { NotificationItemFieldValue, NotificationSettingsFormValues } from "./NotificationSettingsForm";
// TODO(https://github.com/airbytehq/hydra-issues-internal/issues/109): When backend API is ready, use NotificationSettings from "core/api/types/AirbyteClient" instead
import { ExtendedNotificationSettings } from "./types";

const mockNotificationItemFieldValue: NotificationItemFieldValue = {
  slack: false,
  customerio: false,
  slackWebhookLink: "",
};

const mockNotificationItem: NotificationItem = {
  notificationType: [],
};

const mockEmptyFormValues: NotificationSettingsFormValues = {
  sendOnFailure: mockNotificationItemFieldValue,
  sendOnSuccess: mockNotificationItemFieldValue,
  sendOnConnectionUpdate: mockNotificationItemFieldValue,
  sendOnConnectionUpdateActionRequired: mockNotificationItemFieldValue,
  sendOnSyncDisabled: mockNotificationItemFieldValue,
  sendOnSyncDisabledWarning: mockNotificationItemFieldValue,
  sendOnBreakingChangeWarning: mockNotificationItemFieldValue,
  sendOnBreakingChangeSyncsDisabled: mockNotificationItemFieldValue,
  sendOnConnectionSyncQueued: mockNotificationItemFieldValue,
};
const mockEmptyNotificationSettings: ExtendedNotificationSettings = {
  sendOnFailure: mockNotificationItem,
  sendOnSuccess: mockNotificationItem,
  sendOnConnectionUpdate: mockNotificationItem,
  sendOnConnectionUpdateActionRequired: mockNotificationItem,
  sendOnSyncDisabled: mockNotificationItem,
  sendOnSyncDisabledWarning: mockNotificationItem,
  sendOnBreakingChangeWarning: mockNotificationItem,
  sendOnBreakingChangeSyncsDisabled: mockNotificationItem,
  sendOnConnectionSyncQueued: mockNotificationItem,
};

describe("formValuesToNotificationSettings", () => {
  it("converts empty notifications", () => {
    expect(formValuesToNotificationSettings(mockEmptyFormValues)).toEqual(mockEmptyNotificationSettings);
  });

  it("adds slack configuration if slack notifications are enabled", () => {
    const formValues: NotificationSettingsFormValues = {
      ...mockEmptyFormValues,
      sendOnFailure: { ...mockNotificationItemFieldValue, slack: true, slackWebhookLink: "www.airbyte.com" },
    };

    const expectedNotificationSettings: ExtendedNotificationSettings = {
      ...mockEmptyNotificationSettings,
      sendOnFailure: {
        ...mockNotificationItem,
        notificationType: ["slack"],
        slackConfiguration: { webhook: "www.airbyte.com" },
      },
    };

    expect(formValuesToNotificationSettings(formValues)).toEqual(expectedNotificationSettings);
  });

  it("adds customerio and slack if specified", () => {
    const formValues: NotificationSettingsFormValues = {
      ...mockEmptyFormValues,
      sendOnFailure: { ...mockNotificationItemFieldValue, slack: true, slackWebhookLink: "www.airbyte.com" },
      sendOnSuccess: { customerio: true, slack: true, slackWebhookLink: "www.airbyte.io" },
    };

    const expectedNotificationSettings: ExtendedNotificationSettings = {
      ...mockEmptyNotificationSettings,
      sendOnFailure: {
        ...mockNotificationItem,
        notificationType: ["slack"],
        slackConfiguration: { webhook: "www.airbyte.com" },
      },
      sendOnSuccess: {
        ...mockNotificationItem,
        notificationType: ["customerio", "slack"],
        slackConfiguration: { webhook: "www.airbyte.io" },
      },
    };

    expect(formValuesToNotificationSettings(formValues)).toEqual(expectedNotificationSettings);
  });
});
