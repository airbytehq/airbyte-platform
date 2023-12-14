import { useQuery } from "@tanstack/react-query";

import { FeatureItem, useFeature } from "core/services/features";

import { webBackendCheckUpdates } from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { WebBackendCheckUpdatesRead } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

const NO_UPDATES: WebBackendCheckUpdatesRead = {
  destinationDefinitions: 0,
  sourceDefinitions: 0,
};

export const connectorDefinitionKeys = {
  all: [SCOPE_WORKSPACE, "connectorDefinition"] as const,
  count: () => [...connectorDefinitionKeys.all, "count"] as const,
};

export const useGetOutOfDateConnectorsCount = () => {
  const requestOptions = useRequestOptions();
  const allowUpdateConnectors = useFeature(FeatureItem.AllowUpdateConnectors);
  return useQuery(connectorDefinitionKeys.count(), () => {
    if (!allowUpdateConnectors) {
      return Promise.resolve(NO_UPDATES);
    }
    return webBackendCheckUpdates(requestOptions);
  });
};
