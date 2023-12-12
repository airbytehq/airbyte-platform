import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Card } from "components/ui/Card";

import { useCurrentWorkspace, useInvalidateWorkspace } from "core/api";
import { useUpdateCloudWorkspace } from "core/api/cloud";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification";

interface WorkspaceFormValues {
  name: string;
}

const ValidationSchema: SchemaOf<WorkspaceFormValues> = yup.object().shape({
  name: yup.string().required("form.empty.error"),
});

export const GeneralSettingsSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: updateCloudWorkspace } = useUpdateCloudWorkspace();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const { workspaceId, name, email } = useCurrentWorkspace();
  const invalidateWorkspace = useInvalidateWorkspace(workspaceId);

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
    <Card title={<FormattedMessage id="settings.generalSettings" />}>
      <Card withPadding>
        <Form<WorkspaceFormValues>
          defaultValues={{
            name,
          }}
          schema={ValidationSchema}
          onSubmit={onSubmit}
          onSuccess={onSuccess}
          onError={onError}
        >
          <FormControl<WorkspaceFormValues>
            name="name"
            fieldType="input"
            label={formatMessage({ id: "settings.workspaceSettings.updateWorkspaceNameForm.name.label" })}
            placeholder={formatMessage({
              id: "settings.workspaceSettings.updateWorkspaceNameForm.name.placeholder",
            })}
          />
          <FormSubmissionButtons submitKey="form.saveChanges" />
        </Form>
      </Card>
    </Card>
  );
};
