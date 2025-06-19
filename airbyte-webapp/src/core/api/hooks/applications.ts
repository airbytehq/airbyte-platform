import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";

import { useNotificationService } from "hooks/services/Notification";

import {
  applicationTokenRequest,
  createApplication,
  deleteApplication,
  listApplications,
} from "../generated/AirbyteClient";
import {
  ApplicationCreate,
  ApplicationRead,
  ApplicationReadList,
  ApplicationTokenRequest,
} from "../generated/AirbyteClient.schemas";
import { SCOPE_USER } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const applicationKeys = {
  all: [SCOPE_USER, "applications"] as const,
  lists: () => [...applicationKeys.all, "list"],
  detail: (applicationId: string) => [...applicationKeys.all, "details", applicationId] as const,
};

export const useCreateApplication = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  return useMutation(
    (application: ApplicationCreate): Promise<ApplicationRead> => {
      return createApplication(application, requestOptions);
    },
    {
      onSuccess: (data: ApplicationRead) => {
        queryClient.setQueryData<ApplicationReadList>(applicationKeys.lists(), (oldData) => {
          return { applications: [...(oldData?.applications ?? []), data] };
        });
      },
      onError: () => {
        registerNotification({
          id: "settings.application.create.error",
          text: formatMessage({ id: "settings.application.create.error" }),
          type: "error",
        });
      },
    }
  );
};

export const useNonblockingListApplications = () => {
  const requestOptions = useRequestOptions();
  return useQuery(applicationKeys.lists(), () => {
    return listApplications(requestOptions);
  });
};

export const useListApplications = () => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(applicationKeys.lists(), () => {
    return listApplications(requestOptions);
  });
};

export const useDeleteApplication = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  return useMutation(
    async (applicationId: string) => {
      return deleteApplication({ applicationId }, requestOptions);
    },
    {
      onSuccess: (_data, applicationId) => {
        queryClient.setQueryData<ApplicationReadList>(applicationKeys.lists(), (oldData) => {
          return {
            applications: oldData?.applications.filter((application) => application.id !== applicationId) ?? [],
          };
        });
        registerNotification({
          id: "settings.applications.token.deletion.success",
          text: formatMessage({ id: "settings.applications.deletion.success" }),
          type: "success",
        });
      },
      onError: () => {
        registerNotification({
          id: "settings.applications.deletion.error",
          text: formatMessage({ id: "settings.applications.deletion.error" }),
          type: "error",
        });
      },
    }
  );
};

export const useGenerateApplicationToken = () => {
  const requestOptions = useRequestOptions();

  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  return useMutation(
    (request: ApplicationTokenRequest) => {
      return applicationTokenRequest(request, requestOptions);
    },
    {
      onError: () => {
        registerNotification({
          id: "settings.applications.token.generation.error",
          text: formatMessage({ id: "settings.applications.token.generation.error" }),
          type: "error",
        });
      },
    }
  );
};
