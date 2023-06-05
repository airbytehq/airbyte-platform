import { useSuspenseQuery } from "services/connector/useSuspenseQuery";

import { getSpeakeasyCallbackUrl } from "../../generated/AirbyteApi";
import { useRequestOptions } from "../../useRequestOptions";

export const useSpeakeasyRedirect = () => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(["speakeasy-redirect"], () => getSpeakeasyCallbackUrl(requestOptions));
};
