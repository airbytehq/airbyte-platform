import { useMutation } from "@tanstack/react-query";

import { checkOperation } from "../generated/AirbyteClient";
import { CheckOperationReadStatus, OperationCreate } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";

export const useOperationsCheck = () => {
  const requestOptions = useRequestOptions();
  return useMutation(
    ({ operatorConfiguration }: OperationCreate) => checkOperation(operatorConfiguration, requestOptions),
    {
      onSuccess: async (result) => {
        if (result.status === CheckOperationReadStatus.failed) {
          throw new Error(`Operation check failed: ${result.message}`);
        }
      },
    }
  ).mutateAsync;
};
