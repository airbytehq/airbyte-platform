import { listEmbeddedSourceTemplates, getEmbeddedSourceTemplatesId } from "../generated/SonarClient";
import { SourceTemplateListResponse } from "../generated/SonarClient.schemas";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const configTemplates = {
  all: [SCOPE_ORGANIZATION, "configTemplates"] as const,
  lists: () => [...configTemplates.all, "list"],
  detail: (configTemplateId: string) => [...configTemplates.all, "details", configTemplateId] as const,
};

export const useListConfigTemplates = (workspaceId: string): SourceTemplateListResponse => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.lists(), () => {
    return listEmbeddedSourceTemplates({ workspace_id: workspaceId }, requestOptions);
  });
};

export const useGetConfigTemplate = (configTemplateId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.detail(configTemplateId), () => {
    return getEmbeddedSourceTemplatesId(configTemplateId, requestOptions);
  });
};
