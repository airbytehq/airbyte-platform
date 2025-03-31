import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useAuthService } from "core/services/auth";

import { ApiCallOptions } from "./apiCall";

export const emptyGetAccessToken = () => Promise.resolve(null);

export const useRequestOptions = (): ApiCallOptions => {
  const { getAccessToken, authType } = useAuthService();

  return useMemo(
    () => ({
      getAccessToken: getAccessToken ?? emptyGetAccessToken,
      includeCredentials: authType === "simple",
    }),
    [getAccessToken, authType]
  );
};

export const useQueryParamsRequestOptions = (): ApiCallOptions => {
  const [searchParams] = useSearchParams();
  const scopedAuthToken = searchParams.get("auth");

  return useMemo(
    () => ({
      getAccessToken: () => Promise.resolve(scopedAuthToken),
    }),
    [scopedAuthToken]
  );
};
