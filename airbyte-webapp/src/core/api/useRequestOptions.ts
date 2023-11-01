import { useMemo } from "react";

import { useAuthService } from "core/services/auth";

import { ApiCallOptions } from "./apiCall";

export const useRequestOptions = (): ApiCallOptions => {
  const { getAccessToken } = useAuthService();

  return useMemo(
    () => ({
      getAccessToken: getAccessToken ?? (() => Promise.resolve(null)),
    }),
    [getAccessToken]
  );
};
