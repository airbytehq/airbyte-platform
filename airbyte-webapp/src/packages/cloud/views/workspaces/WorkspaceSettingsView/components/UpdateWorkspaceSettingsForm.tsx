import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Form, FormControl } from "components/forms";
import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace, useInvalidateWorkspace, useUpdateWorkspace } from "core/api";
import { Geography } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { trackError } from "core/utils/datadog";
import { useIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

interface WorkspaceFormValues {
  name: string;
  defaultGeography?: Geography;
}

const ValidationSchema: SchemaOf<WorkspaceFormValues> = yup.object().shape({
  name: yup.string().required("form.empty.error"),
  defaultGeography: yup.mixed<Geography>().optional(),
});

export const UpdateWorkspaceSettingsForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const { registerNotification } = useNotificationService();
  const { workspaceId, organizationId, name, email, defaultGeography } = useCurrentWorkspace();
  const invalidateWorkspace = useInvalidateWorkspace(workspaceId);
  const canUpdateWorkspace = useIntent("UpdateWorkspace", { workspaceId, organizationId });
  const supportsDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);

  const onSubmit = async ({ name, defaultGeography }: WorkspaceFormValues) => {
    await updateWorkspace({
      workspaceId,
      name,
      defaultGeography,
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
        defaultGeography,
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
      {supportsDataResidency && <DataResidencyDropdown labelId="settings.defaultGeography" name="defaultGeography" />}
      {canUpdateWorkspace && <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />}
    </Form>
  );
};
