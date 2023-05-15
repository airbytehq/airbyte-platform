import { useMemo } from "react";
import { useParams } from "react-router-dom";

import { StepsTypes } from "components/ConnectorBlocks";

import { useGetSource } from "hooks/services/useSourceHook";

export const useGetSourceFromParams = () => {
  const params = useParams<{ "*": StepsTypes | "" | undefined; sourceId: string }>();
  if (!params.sourceId) {
    throw new Error("Source id is missing");
  }
  return useGetSource(params.sourceId);
};

export const useGetSourceTabFromParams = () => {
  const params = useParams<{ "*": StepsTypes | "" | undefined; sourceId: string }>();

  return useMemo<StepsTypes | undefined>(() => {
    return params["*"] === "" ? StepsTypes.OVERVIEW : params["*"];
  }, [params]);
};
