import { useMutation } from "@tanstack/react-query";

import { tryNotificationConfig } from "../generated/AirbyteClient";
import { Notification } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const useNotificationConfigTest = () => {
  const requestOptions = useRequestOptions();
  return useMutation((notification: Notification) => tryNotificationConfig(notification, requestOptions)).mutateAsync;
};
