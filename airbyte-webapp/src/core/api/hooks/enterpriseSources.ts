import { EnterpriseSourceStubType } from "core/domain/connector";

import { useCurrentWorkspace } from "./workspaces";
import { listEnterpriseSourceStubsForWorkspace } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const enterpriseSourceStubsKeys = {
  all: [SCOPE_ORGANIZATION, "sourceDefinition"] as const,
  lists: () => [...enterpriseSourceStubsKeys.all, "list"] as const,
};

export const useListEnterpriseStubsForWorkspace = () => {
  const requestOptions = useRequestOptions();
  const { workspaceId } = useCurrentWorkspace();

  return useSuspenseQuery(enterpriseSourceStubsKeys.lists(), async () => {
    const enterpriseSourceDefinitions: EnterpriseSourceStubType[] = await listEnterpriseSourceStubsForWorkspace(
      { workspaceId },
      requestOptions
    ).then(({ enterpriseSourceStubs }) =>
      enterpriseSourceStubs
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((stub) => {
          return { ...stub, isEnterprise: true as const };
        })
    );
    const enterpriseSourceDefinitionsMap = new Map<string, EnterpriseSourceStubType>();
    enterpriseSourceDefinitions.forEach((enterpriseSource) => {
      enterpriseSourceDefinitionsMap.set(enterpriseSource.id, enterpriseSource);
    });
    return {
      enterpriseSourceDefinitions,
      enterpriseSourceDefinitionsMap,
    };
  });
};
