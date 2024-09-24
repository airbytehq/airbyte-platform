import { useMutation, useQuery } from "@tanstack/react-query";

import {
  getCustomerPortalLink,
  getOrganizationBalance,
  getPaymentInformation,
  listPastInvoices,
} from "../generated/AirbyteClient";
import { CustomerPortalRequestBody } from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export const billingKeys = {
  all: [SCOPE_ORGANIZATION, "billing"] as const,
  upcomingInvoice: (organizationId: string) => [...billingKeys.all, "upcomingInvoice", organizationId] as const,
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

export const useGetOrganizationBillingBalance = (organizationId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(billingKeys.upcomingInvoice(organizationId), () =>
    getOrganizationBalance({ organizationId }, requestOptions)
  );
};
