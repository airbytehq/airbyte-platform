import { useMutation } from "@tanstack/react-query";

import { useNotificationService } from "hooks/services/Notification";

import { listConfigTemplates, getConfigTemplate, createPartialUserConfig } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { ConfigTemplateList, PartialUserConfigCreate } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const configTemplates = {
  all: [SCOPE_ORGANIZATION, "configTemplates"] as const,
  lists: () => [...configTemplates.all, "list"],
  detail: (configTemplateId: string) => [...configTemplates.all, "details", configTemplateId] as const,
};

export const useListConfigTemplates = (organizationId: string): ConfigTemplateList => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.lists(), () => {
    return listConfigTemplates({ organizationId }, requestOptions);
  });
};

export const useGetConfigTemplate = (configTemplateId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.detail(configTemplateId), () => {
    return getConfigTemplate({ configTemplateId }, requestOptions);
  });
};

export const useCreatePartialUserConfig = () => {
  const requestOptions = useRequestOptions();
  const { registerNotification } = useNotificationService();

  return useMutation(
    async (partialUserConfigCreate: PartialUserConfigCreate) => {
      return await createPartialUserConfig(partialUserConfigCreate, requestOptions);
    },
    {
      onSuccess: () =>
        registerNotification({
          id: "partial-user-config-success",
          type: "success",
          text: "Success!",
        }),
      onError: (error: Error) => {
        registerNotification({
          id: "partial-user-config-error",
          type: "error",
          text: error.message,
        });
      },
    }
  );
};
