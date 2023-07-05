import { useMutation } from "@tanstack/react-query";

import { getLogs } from "../generated/AirbyteClient";
import { LogsRequestBody } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export function useGetLogs() {
  const requestOptions = useRequestOptions();
  return useMutation((payload: LogsRequestBody) => getLogs(payload, requestOptions)).mutateAsync;
}
