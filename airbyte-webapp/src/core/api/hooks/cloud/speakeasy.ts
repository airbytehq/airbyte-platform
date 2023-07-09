import { getSpeakeasyCallbackUrl } from "../../generated/AirbyteApi";
import { useRequestOptions } from "../../useRequestOptions";
import { useSuspenseQuery } from "../../useSuspenseQuery";

export const useSpeakeasyRedirect = () => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(["speakeasy-redirect"], () => getSpeakeasyCallbackUrl(requestOptions));
};
