import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import { useIntl } from "react-intl";

import { HttpError, HttpProblem } from "../errors";
import { webBackendDescribeCronExpression } from "../generated/AirbyteClient";
import { SCOPE_USER } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export const cronKeys = {
  all: [SCOPE_USER, "cron"] as const,
  validateCronExpression: (cronExpression: string) =>
    [...cronKeys.all, "validateCronExpression", cronExpression] as const,
};

export type ValidateCronExpressionResponse =
  | {
      isValid: true;
      cronDescription: string;
      nextExecutions: number[];
    }
  | { isValid: false; validationErrorMessage: string };

function useDescribeCronExpressionApiCall(): (cronExpression: string) => Promise<ValidateCronExpressionResponse> {
  const requestOptions = useRequestOptions();
  const { formatMessage } = useIntl();

  return useCallback(
    async (cronExpression: string) => {
      try {
        const data = await webBackendDescribeCronExpression({ cronExpression }, requestOptions);
        return {
          isValid: true,
          cronDescription: data.description,
          nextExecutions: data.nextExecutions,
        } as const;
      } catch (error) {
        if (error instanceof HttpError && HttpProblem.isType(error, "error:cron-validation/invalid-expression")) {
          return {
            isValid: false,
            validationErrorMessage:
              error.response.data?.validationErrorMessage ?? formatMessage({ id: "form.cronExpression.invalid" }),
          } as const;
        }
        return {
          isValid: false,
          validationErrorMessage: formatMessage({ id: "form.cronExpression.invalid" }),
        } as const;
      }
    },
    [formatMessage, requestOptions]
  );
}

export const useDescribeCronExpression = (cronExpression: string) => {
  const validateCronExpression = useDescribeCronExpressionApiCall();

  return useQuery(cronKeys.validateCronExpression(cronExpression), () => validateCronExpression(cronExpression), {
    staleTime: Infinity,
    cacheTime: Infinity,
  });
};

export const useDescribeCronExpressionFetchQuery = (): ((
  cronExpression: string
) => Promise<ValidateCronExpressionResponse>) => {
  const queryClient = useQueryClient();
  const validateCronExpression = useDescribeCronExpressionApiCall();

  return useCallback(
    (cronExpression: string) =>
      queryClient.fetchQuery({
        queryKey: cronKeys.validateCronExpression(cronExpression),
        queryFn: () => validateCronExpression(cronExpression),
        staleTime: Infinity,
        cacheTime: Infinity,
      }),
    [queryClient, validateCronExpression]
  );
};
