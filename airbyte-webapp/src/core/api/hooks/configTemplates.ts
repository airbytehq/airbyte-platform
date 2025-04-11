import { listConfigTemplates, getConfigTemplate } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { ConfigTemplateList } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const configTemplates = {
  all: [SCOPE_ORGANIZATION, "configTemplates"] as const,
  lists: () => [...configTemplates.all, "list"],
  detail: (configTemplateId: string) => [...configTemplates.all, "details", configTemplateId] as const,
};

export const useListConfigTemplates = (workspaceId: string): ConfigTemplateList => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.lists(), () => {
    return listConfigTemplates({ workspaceId }, requestOptions);
  });
};

export const useGetConfigTemplate = (configTemplateId: string, workspaceId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.detail(configTemplateId), () => {
    return getConfigTemplate({ configTemplateId, workspaceId }, requestOptions);
  });
};
