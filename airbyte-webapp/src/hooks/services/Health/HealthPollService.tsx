import { useEffect, useState } from "react";
import { useIntl } from "react-intl";

import { useHealthCheck } from "core/api";
import { useNotificationService } from "hooks/services/Notification/NotificationService";

import { Notification } from "../Notification";

const HEALTH_NOTIFICATION_ID = "health.error";
const HEALTHCHECK_MAX_COUNT = 3;
const HEALTHCHECK_INTERVAL = 20000;

function useApiHealthPoll(): void {
  const [count, setCount] = useState(0);
  const { formatMessage } = useIntl();
  const healthCheck = useHealthCheck();
  const { registerNotification, unregisterNotificationById } = useNotificationService();

  useEffect(() => {
    const errorNotification: Notification = {
      id: HEALTH_NOTIFICATION_ID,
      text: formatMessage({ id: "notifications.error.health" }),
      type: "error",
    };

    const interval = setInterval(async () => {
      try {
        await healthCheck();
        if (count >= HEALTHCHECK_MAX_COUNT) {
          unregisterNotificationById(HEALTH_NOTIFICATION_ID);
        }
        setCount(0);
      } catch (e) {
        if (count < HEALTHCHECK_MAX_COUNT) {
          setCount((count) => ++count);
        } else {
          registerNotification(errorNotification);
        }
      }
    }, HEALTHCHECK_INTERVAL);

    return () => clearInterval(interval);
  }, [count, formatMessage, unregisterNotificationById, registerNotification, healthCheck]);
}

export { useApiHealthPoll };
