import { useCallback, useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { ToastType } from "components/ui/Toast";

import { useNotificationService } from "hooks/services/Notification";
import { CreateProjectContext, useCreateProject } from "services/connectorBuilder/ConnectorBuilderProjectsService";

import { getEditPath } from "../ConnectorBuilderRoutes";

const CREATE_PROJECT_ERROR_ID = "connectorBuilder.createProject.error";

export const useCreateAndNavigate = () => {
  const { mutateAsync: createProject, isLoading } = useCreateProject();
  const { registerNotification, unregisterNotificationById } = useNotificationService();

  useEffect(
    () => () => {
      unregisterNotificationById(CREATE_PROJECT_ERROR_ID);
    },
    [unregisterNotificationById]
  );
  const navigate = useNavigate();
  const createAndNavigate = useCallback(
    async (context: CreateProjectContext) => {
      try {
        const result = await createProject(context);
        navigate(`../${getEditPath(result.builderProjectId)}`);
      } catch (e) {
        registerNotification({
          id: CREATE_PROJECT_ERROR_ID,
          text: (
            <FormattedMessage
              id={CREATE_PROJECT_ERROR_ID}
              values={{
                reason: e.message,
              }}
            />
          ),
          type: ToastType.ERROR,
        });
      }
    },
    [createProject, navigate, registerNotification]
  );
  return { createAndNavigate, isLoading };
};
