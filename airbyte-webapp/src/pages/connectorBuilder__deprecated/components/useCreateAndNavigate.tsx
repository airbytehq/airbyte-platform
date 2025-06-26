import { useCallback, useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { CreateProjectContext, useCreateBuilderProject, useCreateSourceDefForkedBuilderProject } from "core/api";
import { SourceDefinitionId } from "core/api/types/AirbyteClient";
import { useNotificationService } from "hooks/services/Notification";
import { useConnectorBuilderLocalStorage } from "services/connectorBuilder__deprecated/ConnectorBuilderLocalStorageService";

import { getEditPath } from "../ConnectorBuilderRoutes";

const CREATE_PROJECT_ERROR_ID = "connectorBuilder.createProject.error";

export const useCreateAndNavigate = () => {
  const { mutateAsync: createProject, isLoading: isCreateLoading } = useCreateBuilderProject();
  const { mutateAsync: createForkedProject, isLoading: isCreateForkedLoading } =
    useCreateSourceDefForkedBuilderProject();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { setAssistProjectEnabled } = useConnectorBuilderLocalStorage();

  useEffect(
    () => () => {
      unregisterNotificationById(CREATE_PROJECT_ERROR_ID);
    },
    [unregisterNotificationById]
  );

  const isLoading = useMemo(() => isCreateLoading || isCreateForkedLoading, [isCreateForkedLoading, isCreateLoading]);

  const showErrorToast = useCallback(
    (error: Error) => {
      registerNotification({
        id: CREATE_PROJECT_ERROR_ID,
        text: (
          <FormattedMessage
            id={CREATE_PROJECT_ERROR_ID}
            values={{
              reason: error.message,
            }}
          />
        ),
        type: "error",
      });
    },
    [registerNotification]
  );

  const navigate = useNavigate();
  const navigateToProject = useCallback(
    (projectId: string) => {
      navigate(`../${getEditPath(projectId)}`);
    },
    [navigate]
  );

  const createAndNavigate = useCallback(
    async (context: CreateProjectContext) => {
      try {
        const result = await createProject(context);
        if (context.assistSessionId) {
          setAssistProjectEnabled(result.builderProjectId, true, context.assistSessionId);
        }
        navigateToProject(result.builderProjectId);
      } catch (e) {
        showErrorToast(e);
      }
    },
    [createProject, navigateToProject, showErrorToast, setAssistProjectEnabled]
  );

  const forkAndNavigate = useCallback(
    async (baseSourceDefinitionId: SourceDefinitionId) => {
      try {
        const result = await createForkedProject(baseSourceDefinitionId);
        navigateToProject(result.builderProjectId);
      } catch (e) {
        showErrorToast(e);
      }
    },
    [createForkedProject, navigateToProject, showErrorToast]
  );

  return { createAndNavigate, forkAndNavigate, isLoading };
};
