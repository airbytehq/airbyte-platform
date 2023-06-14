import { useState } from "react";
import { useIntl } from "react-intl";

import { Notification, useNotificationService } from "./services/Notification";

const useLoadingState = (): {
  isLoading: boolean;
  startAction: ({
    action,
    feedbackAction,
  }: {
    action: () => Promise<void>;
    feedbackAction?: () => void;
  }) => Promise<void>;
  showFeedback: boolean;
} => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const [isLoading, setIsLoading] = useState(false);
  const [showFeedback, setShowFeedback] = useState(false);

  const errorNotification: Notification = {
    id: "notifications.error.somethingWentWrong",
    text: formatMessage({ id: `notifications.error.somethingWentWrong` }),
    type: "error",
  };

  const startAction = async ({
    action,
    feedbackAction,
  }: {
    action: () => Promise<void>;
    feedbackAction?: () => void;
  }) => {
    try {
      setIsLoading(true);
      setShowFeedback(false);

      await action();

      setIsLoading(false);
      setShowFeedback(true);

      setTimeout(() => {
        setShowFeedback(false);
        if (feedbackAction) {
          feedbackAction();
        }
      }, 2000);
    } catch {
      setIsLoading(false);
      registerNotification(errorNotification);
    }
  };

  return { isLoading, showFeedback, startAction };
};

export default useLoadingState;
