import React from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace, useInvalidateWorkspace, useUpdateWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { trackError } from "core/utils/datadog";
import { useIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

const ValidationSchema = z.object({
  name: z.string().trim().nonempty("form.empty.error"),
  dataplaneGroupId: z.string().optional(),
});

type WorkspaceFormValues = z.infer<typeof ValidationSchema>;

export const UpdateWorkspaceSettingsForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const { registerNotification } = useNotificationService();
  const { workspaceId, organizationId, name, email, dataplaneGroupId } = useCurrentWorkspace();
  const invalidateWorkspace = useInvalidateWorkspace(workspaceId);
  const canUpdateWorkspace = useIntent("UpdateWorkspace", { workspaceId, organizationId });
  const supportsDataResidency = useFeature(FeatureItem.AllowChangeDataplanes);

  const onSubmit = async ({ name, dataplaneGroupId }: WorkspaceFormValues) => {
    await updateWorkspace({
      workspaceId,
      name,
      dataplaneGroupId,
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
        dataplaneGroupId,
      }}
      zodSchema={ValidationSchema}
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
      {supportsDataResidency && dataplaneGroupId && (
        <DataResidencyDropdown labelId="settings.region" name="dataplaneGroupId" />
      )}
      {canUpdateWorkspace && <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />}
    </Form>
  );
};
