import { useCallback, useEffect } from "react";
import { useIntl } from "react-intl";

import { useNotificationService } from "../../hooks/services/Notification";

const NOTIFICATION_ID = "unexpected-request-error";

export function useRequestErrorHandler(messageId: string) {
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();

  useEffect(() => () => unregisterNotificationById(NOTIFICATION_ID), [unregisterNotificationById]);

  return useCallback(
    (error: unknown) => {
      const values: Record<string, string> = {};
      if (typeof error === "object" && error && "message" in error) {
        values.message = String(error.message);
      } else {
        values.message = String(error);
      }
      registerNotification({
        id: NOTIFICATION_ID,
        type: "error",
        text: formatMessage({ id: messageId }, values),
      });
    },
    [formatMessage, messageId, registerNotification]
  );
}
