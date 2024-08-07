import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace, useUpdateWorkspaceName } from "core/api";
import { trackError } from "core/utils/datadog";
import { useIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

interface UpdateWorkspaceNameFormValues {
  name: string;
}

const schema = yup.object().shape({
  name: yup.string().required("form.empty.error"),
});

export const UpdateWorkspaceNameForm = () => {
  const { name, workspaceId, organizationId } = useCurrentWorkspace();
  const { formatMessage } = useIntl();
  const { mutateAsync: updateWorkspaceName } = useUpdateWorkspaceName();
  const { registerNotification } = useNotificationService();
  const canUpdateWorkspace = useIntent("UpdateWorkspace", { workspaceId, organizationId });

  const onSubmit = async ({ name }: UpdateWorkspaceNameFormValues) => {
    await updateWorkspaceName({
      workspaceId,
      name,
    });
  };

  const onSuccess = () => {
    registerNotification({
      id: "workspace_name_change_success",
      text: formatMessage({ id: "settings.workspaceSettings.updateWorkspaceNameSuccess" }),
      type: "success",
    });
  };

  const onError = (e: Error, { name }: UpdateWorkspaceNameFormValues) => {
    trackError(e, { name });

    registerNotification({
      id: "workspace_name_change_error",
      text: formatMessage({ id: "settings.workspaceSettings.updateWorkspaceNameError" }),
      type: "error",
    });
  };

  return (
    <Form<UpdateWorkspaceNameFormValues>
      defaultValues={{ name }}
      schema={schema}
      onSubmit={onSubmit}
      onError={onError}
      onSuccess={onSuccess}
      disabled={!canUpdateWorkspace}
    >
      <FormControl<UpdateWorkspaceNameFormValues>
        name="name"
        fieldType="input"
        label={formatMessage({ id: "settings.workspaceSettings.updateWorkspaceNameForm.name.label" })}
      />
      {canUpdateWorkspace && <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />}
    </Form>
  );
};
