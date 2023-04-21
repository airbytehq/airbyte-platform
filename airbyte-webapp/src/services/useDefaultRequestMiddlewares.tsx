import { useMemo } from "react";

import { RequestMiddleware } from "core/request/RequestMiddleware";
import { useGetService } from "core/servicesProvider";

/**
 * This hook is responsible for registering RequestMiddlewares used in BaseRequest
 */
export const useDefaultRequestMiddlewares = (): RequestMiddleware[] => {
  const middlewares = useGetService<RequestMiddleware[] | undefined>("DefaultRequestMiddlewares");
  return useMemo(() => middlewares ?? [], [middlewares]);
};
