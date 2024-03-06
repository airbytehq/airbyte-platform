import { NotificationItem, NotificationSettings } from "core/api/types/AirbyteClient";

import { NotificationItemFieldValue, NotificationSettingsFormValues } from "./NotificationSettingsForm";
import { notificationSettingsToFormValues } from "./notificationSettingsToFormValues";

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
};
const mockEmptyNotificationSettings: NotificationSettings = {
  sendOnFailure: mockNotificationItem,
  sendOnSuccess: mockNotificationItem,
  sendOnConnectionUpdate: mockNotificationItem,
  sendOnConnectionUpdateActionRequired: mockNotificationItem,
  sendOnSyncDisabled: mockNotificationItem,
  sendOnSyncDisabledWarning: mockNotificationItem,
  sendOnBreakingChangeWarning: mockNotificationItem,
  sendOnBreakingChangeSyncsDisabled: mockNotificationItem,
};

describe("notificationSettingsToFormValues", () => {
  it("converts empty notifications", () => {
    expect(notificationSettingsToFormValues(mockEmptyNotificationSettings)).toEqual(mockEmptyFormValues);
  });

  it("converts slack notifications", () => {
    const notificationSettings: NotificationSettings = {
      ...mockEmptyNotificationSettings,
      sendOnFailure: {
        ...mockNotificationItem,
        notificationType: ["slack"],
        slackConfiguration: { webhook: "www.airbyte.com" },
      },
    };
    const expectedFormValues: NotificationSettingsFormValues = {
      ...mockEmptyFormValues,
      sendOnFailure: { ...mockNotificationItemFieldValue, slack: true, slackWebhookLink: "www.airbyte.com" },
    };
    expect(notificationSettingsToFormValues(notificationSettings)).toEqual(expectedFormValues);
  });

  it("converts customerio notifications", () => {
    const notificationSettings: NotificationSettings = {
      ...mockEmptyNotificationSettings,
      sendOnFailure: {
        ...mockNotificationItem,
        notificationType: ["customerio"],
      },
    };
    const expectedFormValues: NotificationSettingsFormValues = {
      ...mockEmptyFormValues,
      sendOnFailure: { ...mockNotificationItemFieldValue, customerio: true },
    };
    expect(notificationSettingsToFormValues(notificationSettings)).toEqual(expectedFormValues);
  });
});
