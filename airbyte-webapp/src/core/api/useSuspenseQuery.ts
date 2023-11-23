import { QueryFunction, QueryKey, useQuery, UseQueryOptions } from "@tanstack/react-query";

interface Disabled {
  enabled: false;
}

/**
 * A wrapper around `useQuery` that enables the suspense mode and fixes the types
 * to make it understand, that it will always return the actual data now.
 */
export function useSuspenseQuery<
  TQueryFnData = unknown,
  TError = unknown,
  TData = TQueryFnData,
  TQueryKey extends QueryKey = QueryKey,
>(
  queryKey: TQueryKey,
  queryFn: QueryFunction<TQueryFnData, TQueryKey>,
  options: Readonly<
    Omit<UseQueryOptions<TQueryFnData, TError, TData, TQueryKey>, "queryKey" | "queryFn" | "suspense">
  > = {}
) {
  return useQuery<TQueryFnData, TError, TData, TQueryKey>(queryKey, queryFn, {
    ...options,
    suspense: true,
  }).data as typeof options extends Disabled ? TData | undefined : TData;
}
