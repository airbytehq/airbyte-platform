import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";

import { useTryNotificationWebhook } from "core/api";
import { NotificationReadStatus, NotificationSettings } from "core/api/types/AirbyteClient";
import { useNotificationService } from "hooks/services/Notification";

import { notificationTriggerMap } from "./NotificationSettingsForm";

interface TestWebhookButtonProps {
  webhookUrl: string;
  disabled: boolean;
  notificationTrigger: keyof NotificationSettings;
}

export const TestWebhookButton: React.FC<TestWebhookButtonProps> = ({ webhookUrl, disabled, notificationTrigger }) => {
  const [isLoading, setIsLoading] = useState(false);
  const { registerNotification, unregisterAllNotifications } = useNotificationService();
  const testWebhook = useTryNotificationWebhook();
  const { formatMessage } = useIntl();

  const onButtonClick = async () => {
    setIsLoading(true);
    unregisterAllNotifications();
    const notificationTest = await testWebhook({
      notificationTrigger: notificationTriggerMap[notificationTrigger],
      slackConfiguration: { webhook: webhookUrl },
    }).catch(() => {
      return { status: NotificationReadStatus.failed };
    });

    if (notificationTest.status === NotificationReadStatus.succeeded) {
      registerNotification({
        type: "success",
        text: formatMessage({ id: "settings.webhook.test.passed" }),
        id: `test_notification_webhook`,
      });
    } else {
      registerNotification({
        type: "error",
        text: formatMessage({ id: "settings.webhook.test.failed" }),
        id: `test_notification_webhook`,
      });
    }

    setIsLoading(false);
  };

  return (
    <Button disabled={disabled} isLoading={isLoading} onClick={onButtonClick} variant="secondary" type="button">
      <FormattedMessage id="settings.notifications.testWebhookButtonLabel" />
    </Button>
  );
};
