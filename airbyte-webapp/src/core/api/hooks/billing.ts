import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  getCustomerPortalLink,
  getSubscriptionInfo,
  getPaymentInformation,
  listPastInvoices,
  cancelSubscription,
  unscheduleCancelSubscription,
} from "../generated/AirbyteClient";
import { CustomerPortalRequestBody } from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export const billingKeys = {
  all: [SCOPE_ORGANIZATION, "billing"] as const,
  subscriptionInfo: (organizationId: string) => [...billingKeys.all, "subscriptionInfo", organizationId] as const,
  invoices: (organizationId: string) => [...billingKeys.all, "invoices", organizationId] as const,
  paymentMethod: (organizationId: string) => [...billingKeys.all, "paymentMethod", organizationId] as const,
};

export const useGetCustomerPortalUrl = () => {
  const requestOptions = useRequestOptions();

  return useMutation((body: CustomerPortalRequestBody) => {
    return getCustomerPortalLink(body, requestOptions);
  });
};

export const useGetInvoices = (organizationId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(billingKeys.invoices(organizationId), () =>
    listPastInvoices({ organizationId }, requestOptions).then((response) => ({
      ...response,
      invoices: response.invoices.map((invoice) => ({
        ...invoice,
        status: invoice.status === "uncollectible" ? "open" : invoice.status,
      })),
    }))
  );
};

export const useGetPaymentInformation = (organizationId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(billingKeys.paymentMethod(organizationId), () =>
    getPaymentInformation({ organizationId }, requestOptions)
  );
};

export const useGetOrganizationSubscriptionInfo = (organizationId: string, enabled: boolean = true) => {
  const requestOptions = useRequestOptions();

  return useQuery(
    billingKeys.subscriptionInfo(organizationId),
    () => getSubscriptionInfo({ organizationId }, requestOptions),
    { enabled }
  );
};

export const useCancelSubscription = (organizationId: string) => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(async () => {
    const response = await cancelSubscription({ organizationId }, requestOptions);
    await queryClient.invalidateQueries(billingKeys.subscriptionInfo(organizationId));
    return response;
  });
};

export const useUnscheduleCancelSubscription = (organizationId: string) => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(async () => {
    const response = await unscheduleCancelSubscription({ organizationId }, requestOptions);
    await queryClient.invalidateQueries(billingKeys.subscriptionInfo(organizationId));
    return response;
  });
};
