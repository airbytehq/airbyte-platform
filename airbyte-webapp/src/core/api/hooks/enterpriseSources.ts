import { EnterpriseConnectorStubType } from "core/domain/connector";

import { useCurrentWorkspace } from "./workspaces";
import { listEnterpriseSourceStubsForWorkspace } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const enterpriseSourceStubsKeys = {
  all: [SCOPE_ORGANIZATION, "sourceDefinition"] as const,
  lists: () => [...enterpriseSourceStubsKeys.all, "list"] as const,
};

export const useListEnterpriseSourceStubs = (options?: { enabled?: boolean }) => {
  const requestOptions = useRequestOptions();
  const { workspaceId } = useCurrentWorkspace();

  return useSuspenseQuery(
    enterpriseSourceStubsKeys.lists(),
    async () => {
      const enterpriseSourceDefinitions: EnterpriseConnectorStubType[] = await listEnterpriseSourceStubsForWorkspace(
        { workspaceId },
        requestOptions
      ).then(({ enterpriseConnectorStubs }) =>
        enterpriseConnectorStubs
          .sort((a, b) => a.name.localeCompare(b.name))
          .map((stub) => {
            return { ...stub, isEnterprise: true as const };
          })
      );
      const enterpriseSourceDefinitionsMap = new Map<string, EnterpriseConnectorStubType>();
      enterpriseSourceDefinitions.forEach((enterpriseSource) => {
        enterpriseSourceDefinitionsMap.set(enterpriseSource.id, enterpriseSource);
      });
      return {
        enterpriseSourceDefinitions,
        enterpriseSourceDefinitionsMap,
      };
    },
    {
      enabled: options?.enabled ?? true,
    }
  );
};
