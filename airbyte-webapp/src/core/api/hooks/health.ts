import { useMutation } from "react-query";

import { getHealthCheck } from "../generated/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const useHealthCheck = () => {
  const requestOptions = useRequestOptions();
  return useMutation(() => getHealthCheck(requestOptions)).mutateAsync;
};
