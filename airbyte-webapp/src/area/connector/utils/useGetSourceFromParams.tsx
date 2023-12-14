import { useParams, useSearchParams } from "react-router-dom";

import { StepsTypes } from "components/ConnectorBlocks";

import { useGetSource } from "core/api";

export const useGetSourceFromParams = () => {
  const params = useParams<{ "*": StepsTypes | "" | undefined; sourceId: string }>();
  if (!params.sourceId) {
    throw new Error("Source id is missing");
  }
  return useGetSource(params.sourceId);
};

export const useGetSourceFromSearchParams = () => {
  const [searchParams] = useSearchParams();
  const sourceId = searchParams.get("sourceId");
  if (!sourceId) {
    throw new Error("Source id is missing");
  }
  return useGetSource(sourceId);
};
