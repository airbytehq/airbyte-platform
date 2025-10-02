import { EnterpriseConnectorStubType } from "core/domain/connector";

import { useCurrentWorkspace } from "./workspaces";
import { listEnterpriseDestinationStubsForWorkspace } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const enterpriseDestinationStubsKeys = {
  all: [SCOPE_ORGANIZATION, "destinationDefinition"] as const,
  lists: () => [...enterpriseDestinationStubsKeys.all, "list"] as const,
};

export const useListEnterpriseDestinationStubs = (options?: { enabled?: boolean }) => {
  const requestOptions = useRequestOptions();
  const { workspaceId } = useCurrentWorkspace();

  return useSuspenseQuery(
    enterpriseDestinationStubsKeys.lists(),
    async () => {
      const enterpriseDestinationDefinitions: EnterpriseConnectorStubType[] =
        await listEnterpriseDestinationStubsForWorkspace({ workspaceId }, requestOptions).then(
          ({ enterpriseConnectorStubs }) =>
            enterpriseConnectorStubs
              .sort((a, b) => a.name.localeCompare(b.name))
              .map((stub) => {
                return { ...stub, isEnterprise: true as const };
              })
        );
      const enterpriseDestinationDefinitionsMap = new Map<string, EnterpriseConnectorStubType>();
      enterpriseDestinationDefinitions.forEach((enterpriseDestination) => {
        enterpriseDestinationDefinitionsMap.set(enterpriseDestination.id, enterpriseDestination);
      });
      return {
        enterpriseDestinationDefinitions,
        enterpriseDestinationDefinitionsMap,
      };
    },
    {
      enabled: options?.enabled ?? true,
    }
  );
};
