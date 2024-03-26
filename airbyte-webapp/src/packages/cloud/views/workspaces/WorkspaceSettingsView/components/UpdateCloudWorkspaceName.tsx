import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace, useInvalidateWorkspace } from "core/api";
import { useUpdateCloudWorkspace } from "core/api/cloud";
import { useIntent } from "core/utils/rbac";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification";

interface WorkspaceFormValues {
  name: string;
}

const ValidationSchema: SchemaOf<WorkspaceFormValues> = yup.object().shape({
  name: yup.string().required("form.empty.error"),
});

export const UpdateCloudWorkspaceName: React.FC = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: updateCloudWorkspace } = useUpdateCloudWorkspace();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const { workspaceId, name, email } = useCurrentWorkspace();
  const invalidateWorkspace = useInvalidateWorkspace(workspaceId);
  const canUpdateWorkspace = useIntent("UpdateWorkspace", { workspaceId });

  const onSubmit = async ({ name }: WorkspaceFormValues) => {
    await updateCloudWorkspace({
      workspaceId,
      name,
    });

    await invalidateWorkspace();
  };

  const onSuccess = () => {
    registerNotification({
      id: "workspace_settings_update_success",
      text: formatMessage({ id: "settings.workspaceSettings.update.success" }),
      type: "success",
    });
  };

  const onError = (e: Error, { name }: WorkspaceFormValues) => {
    trackError(e, { name, email });

    registerNotification({
      id: "workspace_settings_update_error",
      text: formatMessage({ id: "settings.workspaceSettings.update.error" }),
      type: "error",
    });
  };

  return (
    <Form<WorkspaceFormValues>
      defaultValues={{
        name,
      }}
      schema={ValidationSchema}
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
      disabled={!canUpdateWorkspace}
    >
      <FormControl<WorkspaceFormValues>
        name="name"
        fieldType="input"
        label={formatMessage({ id: "settings.workspaceSettings.updateWorkspaceNameForm.name.label" })}
        placeholder={formatMessage({
          id: "settings.workspaceSettings.updateWorkspaceNameForm.name.placeholder",
        })}
      />
      {canUpdateWorkspace && <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />}
    </Form>
  );
};
