import { useMutation } from "@tanstack/react-query";

import { diffCatalogs } from "../generated/AirbyteClient";
import { DiffCatalogsBody } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const useCatalogDiffMutation = () => {
  const requestOptions = useRequestOptions();
  return useMutation((body: DiffCatalogsBody) => diffCatalogs(body, requestOptions));
};
