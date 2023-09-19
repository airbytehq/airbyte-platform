import { useCallback, useMemo } from "react";

import {
  completeDestinationOAuth,
  completeSourceOAuth,
  getDestinationOAuthConsent,
  getSourceOAuthConsent,
  revokeSourceOAuthTokens,
} from "../generated/AirbyteClient";
import {
  CompleteDestinationOAuthRequest,
  CompleteSourceOauthRequest,
  DestinationOauthConsentRequest,
  RevokeSourceOauthTokensRequest,
  SourceOauthConsentRequest,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export function useConsentUrls() {
  const requestOptions = useRequestOptions();

  return useMemo(
    () => ({
      getSourceConsentUrl(request: SourceOauthConsentRequest) {
        return getSourceOAuthConsent(request, requestOptions);
      },
      getDestinationConsentUrl(request: DestinationOauthConsentRequest) {
        return getDestinationOAuthConsent(request, requestOptions);
      },
    }),
    [requestOptions]
  );
}

export function useCompleteOAuth() {
  const requestOptions = useRequestOptions();
  return useMemo(
    () => ({
      completeSourceOAuth(request: CompleteSourceOauthRequest) {
        return completeSourceOAuth(request, requestOptions);
      },
      completeDestinationOAuth(request: CompleteDestinationOAuthRequest) {
        return completeDestinationOAuth(request, requestOptions);
      },
    }),
    [requestOptions]
  );
}

export function useRevokeSourceOAuthToken() {
  const requestOptions = useRequestOptions();
  return useCallback(
    (request: RevokeSourceOauthTokensRequest) => {
      return revokeSourceOAuthTokens(request, requestOptions);
    },
    [requestOptions]
  );
}
