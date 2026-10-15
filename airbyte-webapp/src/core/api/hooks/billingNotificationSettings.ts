import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { getBillingNotificationSettings, updateBillingNotificationSettings } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { BillingNotificationSettingsUpdateRequestBody } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const billingNotificationSettingsKeys = {
  all: [SCOPE_ORGANIZATION, "billingNotificationSettings"] as const,
  detail: (organizationId: string) => [...billingNotificationSettingsKeys.all, organizationId] as const,
};

export const useGetBillingNotificationSettings = (organizationId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(billingNotificationSettingsKeys.detail(organizationId), () =>
    getBillingNotificationSettings({ organizationId }, requestOptions)
  );
};

export const useUpdateBillingNotificationSettings = (organizationId: string) => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    (settings: Omit<BillingNotificationSettingsUpdateRequestBody, "organizationId">) =>
      updateBillingNotificationSettings({ organizationId, ...settings }, requestOptions),
    {
      onSuccess: (savedSettings) => {
        queryClient.setQueryData(billingNotificationSettingsKeys.detail(organizationId), savedSettings);
      },
    }
  );
};
