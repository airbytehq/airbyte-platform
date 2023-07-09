import { useMutation } from "@tanstack/react-query";

import { createStripeCheckoutSession } from "core/api/generated/CloudApi";
import { StripeCheckoutSessionCreate } from "core/api/types/CloudApi";
import { useRequestOptions } from "core/api/useRequestOptions";

export const useStripeCheckout = () => {
  const requestOptions = useRequestOptions();
  return useMutation((params: StripeCheckoutSessionCreate) => createStripeCheckoutSession(params, requestOptions));
};
