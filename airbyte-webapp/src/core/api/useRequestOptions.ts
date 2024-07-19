import { useMemo } from "react";

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
