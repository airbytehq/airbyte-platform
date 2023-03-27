import { useMemo } from "react";

import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";

import { ApiCallOptions } from "./apiCall";

export const useRequestOptions = (): ApiCallOptions => {
  const middlewares = useDefaultRequestMiddlewares();

  return useMemo(
    () => ({
      middlewares,
    }),
    [middlewares]
  );
};
