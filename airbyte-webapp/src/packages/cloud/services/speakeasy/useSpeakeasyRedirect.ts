import { getSpeakeasyCallbackUrl } from "packages/cloud/lib/domain/speakeasy";
import { useSuspenseQuery } from "services/connector/useSuspenseQuery";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";

const SPEAKEASY_QUERY_KEY = "speakeasy-redirect";

export const useSpeakeasyRedirect = () => {
  const middlewares = useDefaultRequestMiddlewares();
  const requestOptions = { middlewares };

  return useSuspenseQuery([SPEAKEASY_QUERY_KEY], () => getSpeakeasyCallbackUrl(requestOptions));
};
