import { useQuery } from "@tanstack/react-query";

import { EnterpriseSourceStubType, EnterpriseSources } from "core/domain/connector";

import { listEnterpriseSourceStubs } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export const enterpriseSourceStubsKeys = {
  all: [SCOPE_ORGANIZATION, "sourceDefinition"] as const,
  lists: () => [...enterpriseSourceStubsKeys.all, "list"] as const,
};

export const useEnterpriseSourceStubsList = (): EnterpriseSources => {
  const requestOptions = useRequestOptions();

  return useQuery(
    enterpriseSourceStubsKeys.lists(),
    async () => {
      const enterpriseSourceDefinitions: EnterpriseSourceStubType[] = await listEnterpriseSourceStubs(
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
    },
    { suspense: true, staleTime: Infinity }
  ).data as EnterpriseSources;
};
