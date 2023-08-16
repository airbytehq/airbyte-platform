import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useSelectWorkspace } from "area/workspace/utils";
import { useCurrentWorkspace, useInvalidateWorkspace } from "core/api";
import { useUpdateCloudWorkspace } from "core/api/cloud";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useNotificationService } from "hooks/services/Notification";

const ValidationSchema = yup.object().shape({
  name: yup.string().required("form.empty.error"),
});

interface WorkspaceFormValues {
  name: string;
}

export const GeneralSettingsSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: updateCloudWorkspace } = useUpdateCloudWorkspace();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const selectWorkspace = useSelectWorkspace();
  const workspace = useCurrentWorkspace();
  const invalidateWorkspace = useInvalidateWorkspace(workspace.workspaceId);

  const onSubmit = async (payload: WorkspaceFormValues) => {
    const { workspaceId } = workspace;
    await updateCloudWorkspace({
      workspaceId,
      name: payload.name,
    });
    await invalidateWorkspace();
  };

  const onSuccess = () => {
    registerNotification({
      id: "workspace_name_change_success",
      text: formatMessage({ id: "settings.workspaceSettings.updateWorkspaceNameSuccess" }),
      type: "success",
    });
  };

  const onError = (e: Error, { name }: WorkspaceFormValues) => {
    trackError(e, { name });

    registerNotification({
      id: "workspace_name_change_error",
      text: formatMessage({ id: "settings.workspaceSettings.updateWorkspaceNameError" }),
      type: "error",
    });
  };

  return (
    <Card
      title={
        <FlexContainer justifyContent="space-between">
          <FormattedMessage id="settings.generalSettings" />
          <Button type="button" onClick={() => selectWorkspace(null)} data-testid="button.changeWorkspace">
            <FormattedMessage id="settings.generalSettings.changeWorkspace" />
          </Button>
        </FlexContainer>
      }
    >
      <Card withPadding>
        <Form<WorkspaceFormValues>
          defaultValues={{
            name: workspace.name,
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
