import { useMutation } from "@tanstack/react-query";

import { tryNotificationConfig, tryNotificationWebhookConfig } from "../generated/AirbyteClient";
import { Notification, NotificationWebhookConfigValidationRequestBody } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

/**
 * @deprecated use useTryNotificationWebhook instead
 **/
export const useNotificationConfigTest = () => {
  const requestOptions = useRequestOptions();
  return useMutation((notification: Notification) => tryNotificationConfig(notification, requestOptions)).mutateAsync;
};

export const useTryNotificationWebhook = () => {
  const requestOptions = useRequestOptions();
  return useMutation((notification: NotificationWebhookConfigValidationRequestBody) =>
    tryNotificationWebhookConfig(notification, requestOptions)
  ).mutateAsync;
};
