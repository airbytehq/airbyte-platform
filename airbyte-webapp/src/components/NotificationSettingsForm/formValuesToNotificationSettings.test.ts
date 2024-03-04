import { NotificationItem, NotificationSettings } from "core/api/types/AirbyteClient";

import { formValuesToNotificationSettings } from "./formValuesToNotificationSettings";
import { NotificationItemFieldValue, NotificationSettingsFormValues } from "./NotificationSettingsForm";

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

describe("formValuesToNotificationSettings", () => {
  it("converts empty notifications", () => {
    expect(formValuesToNotificationSettings(mockEmptyFormValues)).toEqual(mockEmptyNotificationSettings);
  });

  it("adds slack configuration if slack notifications are enabled", () => {
    const formValues: NotificationSettingsFormValues = {
      ...mockEmptyFormValues,
      sendOnFailure: { ...mockNotificationItemFieldValue, slack: true, slackWebhookLink: "www.airbyte.com" },
    };

    const expectedNotificationSEttings: NotificationSettings = {
      ...mockEmptyNotificationSettings,
      sendOnFailure: {
        ...mockNotificationItem,
        notificationType: ["slack"],
        slackConfiguration: { webhook: "www.airbyte.com" },
      },
    };

    expect(formValuesToNotificationSettings(formValues)).toEqual(expectedNotificationSEttings);
  });

  it("adds customerio and slack if specified", () => {
    const formValues: NotificationSettingsFormValues = {
      ...mockEmptyFormValues,
      sendOnFailure: { ...mockNotificationItemFieldValue, slack: true, slackWebhookLink: "www.airbyte.com" },
      sendOnSuccess: { customerio: true, slack: true, slackWebhookLink: "www.airbyte.io" },
    };

    const expectedNotificationSEttings: NotificationSettings = {
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

    expect(formValuesToNotificationSettings(formValues)).toEqual(expectedNotificationSEttings);
  });
});
