import { useMemo } from "react";
import { useParams } from "react-router-dom";

import { StepsTypes } from "components/ConnectorBlocks";

import { useGetDestination } from "hooks/services/useDestinationHook";

export const useGetDestinationFromParams = () => {
  const params = useParams<{ "*": StepsTypes | "" | undefined; destinationId: string }>();
  if (!params.destinationId) {
    throw new Error("Destination id is missing");
  }

  return useGetDestination(params.destinationId);
};

export const useGetDestinationTabFromParams = () => {
  const params = useParams<{ "*": StepsTypes | "" | undefined; destinationId: string }>();

  return useMemo<StepsTypes | undefined>(() => {
    return params["*"] === "" ? StepsTypes.OVERVIEW : params["*"];
  }, [params]);
};
