import { useCallback, useMemo } from "react";

import {
  completeDestinationOAuth,
  completeSourceOAuth,
  getDestinationOAuthConsent,
  getEmbeddedSourceOAuthConsent,
  getSourceOAuthConsent,
  revokeSourceOAuthTokens,
  getConnectorBuilderProjectOAuthConsent,
  completeConnectorBuilderProjectOauth,
} from "../generated/AirbyteClient";
import {
  CompleteDestinationOAuthRequest,
  CompleteSourceOauthRequest,
  DestinationOauthConsentRequest,
  EmbeddedSourceOauthConsentRequest,
  RevokeSourceOauthTokensRequest,
  SourceOauthConsentRequest,
  BuilderProjectOauthConsentRequest,
  CompleteConnectorBuilderProjectOauthRequest,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export function useConsentUrls() {
  const requestOptions = useRequestOptions();

  return useMemo(
    () => ({
      getSourceConsentUrl(request: SourceOauthConsentRequest) {
        return getSourceOAuthConsent(request, requestOptions);
      },
      getEmbeddedSourceConsentUrl(request: EmbeddedSourceOauthConsentRequest) {
        return getEmbeddedSourceOAuthConsent(request, requestOptions);
      },
      getDestinationConsentUrl(request: DestinationOauthConsentRequest) {
        return getDestinationOAuthConsent(request, requestOptions);
      },
    }),
    [requestOptions]
  );
}

export function useConsentUrlsBuilder() {
  const requestOptions = useRequestOptions();

  return useMemo(
    () => ({
      getConsentUrl(request: BuilderProjectOauthConsentRequest) {
        return getConnectorBuilderProjectOAuthConsent(request, requestOptions);
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

export function useCompleteOAuthBuilder() {
  const requestOptions = useRequestOptions();
  return useMemo(
    () => ({
      completeOAuth(request: CompleteConnectorBuilderProjectOauthRequest) {
        return completeConnectorBuilderProjectOauth(request, requestOptions);
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
