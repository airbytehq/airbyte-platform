import { useMutation } from "@tanstack/react-query";

import { tryNotificationWebhookConfig } from "../generated/AirbyteClient";
import { NotificationWebhookConfigValidationRequestBody } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const useTryNotificationWebhook = () => {
  const requestOptions = useRequestOptions();
  return useMutation((notification: NotificationWebhookConfigValidationRequestBody) =>
    tryNotificationWebhookConfig(notification, requestOptions)
  ).mutateAsync;
};
