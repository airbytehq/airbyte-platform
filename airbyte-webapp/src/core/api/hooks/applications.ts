import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";

import { useNotificationService } from "hooks/services/Notification";

import {
  AccessToken,
  ApplicationCreate,
  ApplicationRead,
  ApplicationReadList,
  ApplicationTokenRequest,
} from "../generated/AirbyteClient.schemas";
import { SCOPE_USER } from "../scopes";
import { useSuspenseQuery } from "../useSuspenseQuery";

const applicationKeys = {
  all: [SCOPE_USER, "applications"] as const,
  lists: () => [...applicationKeys.all, "list"],
  detail: (applicationId: string) => [...applicationKeys.all, "details", applicationId] as const,
};

export const useCreateApplication = () => {
  // const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  return useMutation(
    (application: ApplicationCreate): Promise<ApplicationRead> => {
      // return createApplication(application, requestOptions); -- todo: endpoint not implemented yet
      return createMockApplication(application);
    },
    {
      onSuccess: (data: ApplicationRead) => {
        queryClient.setQueryData<ApplicationReadList>(applicationKeys.lists(), (oldData) => {
          return { applications: [...(oldData?.applications ?? []), data] };
        });
      },
      onError: () => {
        registerNotification({
          id: "settings.accessManagement.permissionCreate.error",
          text: formatMessage({ id: "settings.accessManagement.permissionCreate.error" }),
          type: "error",
        });
      },
    }
  );
};

export const useListApplications = () => {
  // const requestOptions = useRequestOptions();
  return useSuspenseQuery(applicationKeys.lists(), () => {
    // return listApplications(requestOptions) -- todo: endpoint not implemented yet
    return listMockApplications();
    // return listMockApplicationsEmpty;
  });
};

export const useDeleteApplication = () => {
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  return useMutation(
    async (applicationId: string) => {
      // return  deleteApplication(applicationId, requestOptions)
      return await deleteMockApplication(applicationId);
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
  // const requestOptions = useRequestOptions();

  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  return useMutation(
    (request: ApplicationTokenRequest) => {
      // return applicationTokenRequest(request, requestOptions),
      return mockGetApplicationToken(request);
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

/**
 * MOCK FUNCTIONS TO BE REMOVED WHEN REAL ENDPOINTS ARE IMPLEMENTED
 */

const createMockApplication = (application: ApplicationCreate) => {
  console.log(application);

  return new Promise<ApplicationRead>((resolve) => {
    setTimeout(
      () =>
        resolve({
          id: "123",
          name: "Super neat application",
          clientId: "1232543252342",
          clientSecret: "myOtherVerySecretSecret",
          createdAt: 1702606603,
        }),
      1000
    );
  });
};

export const listMockApplicationsEmpty = () => {
  return new Promise<ApplicationReadList>((resolve) => {
    setTimeout(() => resolve({ applications: [] }), 1000);
  });
};

export const listMockApplications = () => {
  return new Promise<ApplicationReadList>((resolve) => {
    setTimeout(
      () =>
        resolve({
          applications: [
            {
              id: "123",
              name: "Super neat application",
              clientId: "1232543252342",
              clientSecret: "myOtherVerySecretSecret",
              createdAt: 1702606603,
            },
          ],
        }),
      1000
    );
  });
};

export const deleteMockApplication = (applicationId: string) => {
  console.log(applicationId);
  return new Promise<void>((resolve) => {
    resolve();
  });
};

export const mockGetApplicationToken = (request: ApplicationTokenRequest) => {
  console.log(request);
  return new Promise<AccessToken>((resolve) => {
    setTimeout(
      () =>
        resolve({
          access_token:
            "shorterTokenGBML!LBh/42rWdFl1SEx3vXSBdooD-PVgRqFVpYaiJ7-f/KBqBjTfk2kWbavRX9clQrpqbwCP/?C1GRi0lgvKZAkp7KoLu9gAX!k5lmUklq8Gkw5HzL3XnDS1LM95CIl6A0a1ndGCNIKu4/5ttEMuw8-XA=SOtuVv2sf30X8VDFBPWS4OavQosd8RrEL?TM7SBbVrkM7nH0quL6=PVHztNMyq3Zpyjw3NN!CFQO!ksf9=P/vMVMp!IvX6F/?l8-0",

          // access_token:
          //   "muchLongerTokenGBML!LBh/42rWdFl1SEx3vXSBdooD-PVgRqFVpYaiJ7-f/KBqBjTfk2kWbavRX9clQrpqbwCP/?C1GRi0lgvKZAkp7KoLu9gAX!k5lmUklq8Gkw5HzL3XnDS1LM95CIl6A0a1ndGCNIKu4/5ttEMuw8-XA=SOtuVv2sf30X8VDFBPWS4OavQosd8RrEL?TM7SBbVrkM7nH0quL6=PVHztNMyq3Zpyjw3NN!CFQO!ksf9=P/vMVMp!IvX6F/?l8-0GBML!LBh/42rWdFl1SEx3vXSBdooD-PVgRqFVpYaiJ7-f/KBqBjTfk2kWbavRX9clQrpqbwCP/?C1GRi0lgvKZAkp7KoLu9gAX!k5lmUklq8Gkw5HzL3XnDS1LM95CIl6A0a1ndGCNIKu4/5ttEMuw8-XA=SOtuVv2sf30X8VDFBPWS4OavQosd8RrEL?TM7SBbVrkM7nH0quL6=PVHztNMyq3Zpyjw3NN!CFQO!ksf9=P/vMVMp!IvX6F/?l8-0GBML!LBh/42rWdFl1SEx3vXSBdooD-PVgRqFVpYaiJ7-f/KBqBjTfk2kWbavRX9clQrpqbwCP/?C1GRi0lgvKZAkp7KoLu9gAX!k5lmUklq8Gkw5HzL3XnDS1LM95CIl6A0a1ndGCNIKu4/5ttEMuw8-XA=SOtuVv2sf30X8VDFBPWS4OavQosd8RrEL?TM7SBbVrkM7nH0quL6=PVHztNMyq3Zpyjw3NN!CFQO!ksf9=P/vMVMp!IvX6F/?l8-0GBML!LBh/42rWdFl1SEx3vXSBdooD-PVgRqFVpYaiJ7-f/KBqBjTfk2kWbavRX9clQrpqbwCP/?C1GRi0lgvKZAkp7KoLu9gAX!k5lmUklq8Gkw5HzL3XnDS1LM95CIl6A0a1ndGCNIKu4/5ttEMuw8-XA=SOtuVv2sf30X8VDFBPWS4OavQosd8RrEL?TM7SBbVrkM7nH0quL6=PVHztNMyq3Zpyjw3NN!CFQO!ksf9=P/vMVMp!IvX6F/?l8-0GBML!LBh/42rWdFl1SEx3vXSBdooD-PVgRqFVpYaiJ7-f/KBqBjTfk2kWbavRX9clQrpqbwCP/?C1GRi0lgvKZAkp7KoLu9gAX!k5lmUklq8Gkw5HzL3XnDS1LM95CIl6A0a1ndGCNIKu4/5ttEMuw8-XA=SOtuVv2sf30X8VDFBPWS4OavQosd8RrEL?TM7SBbVrkM7nH0quL6=PVHztNMyq3Zpyjw3NN!CFQO!ksf9=P/vMVMp!IvX6F/?l8-0GBML!LBh/42rWdFl1SEx3vXSBdooD-PVgRqFVpYaiJ7-f/KBqBjTfk2kWbavRX9clQrpqbwCP/?C1GRi0lgvKZAkp7KoLu9gAX!k5lmUklq8Gkw5HzL3XnDS1LM95CIl6A0a1ndGCNIKu4/5ttEMuw8-XA=SOtuVv2sf30X8VDFBPWS4OavQosd8RrEL?TM7SBbVrkM7nH0quL6=PVHztNMyq3Zpyjw3NN!CFQO!ksf9=P/vMVMp!IvX6F/?l8-0",
        }),
      1000
    );
  });
};
