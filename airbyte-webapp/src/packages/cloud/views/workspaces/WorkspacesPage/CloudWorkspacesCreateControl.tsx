import { UseMutateAsyncFunction } from "@tanstack/react-query";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import { useToggle } from "react-use";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Icon } from "components/ui/Icon";

import { useListWorkspaces } from "core/api";
import { CloudWorkspaceRead } from "core/api/types/CloudApi";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";
import styles from "pages/workspaces/components/WorkspacesCreateControl.module.scss";

interface CreateCloudWorkspaceFormValues {
  name: string;
}

const CreateCloudWorkspaceFormValidationSchema = yup.object().shape({
  name: yup.string().trim().required("form.empty.error"),
});

interface CloudWorkspacesCreateControlProps {
  createWorkspace: UseMutateAsyncFunction<CloudWorkspaceRead, unknown, string, unknown>;
}

export const CloudWorkspacesCreateControl: React.FC<CloudWorkspacesCreateControlProps> = ({ createWorkspace }) => {
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const [isEditMode, toggleMode] = useToggle(false);
  const { registerNotification } = useNotificationService();
  const { workspaces } = useListWorkspaces();
  const isFirstWorkspace = workspaces.length === 0;

  const onSubmit = async (values: CreateCloudWorkspaceFormValues) => {
    const newWorkspace = await createWorkspace(values.name);
    toggleMode();
    navigate(`/workspaces/${newWorkspace.workspaceId}`);
  };

  const onSuccess = () => {
    registerNotification({
      id: "workspaces.createSuccess",
      text: formatMessage({ id: "workspaces.createSuccess" }),
      type: "success",
    });
  };

  const onError = (e: Error, { name }: CreateCloudWorkspaceFormValues) => {
    trackError(e, { name });
    registerNotification({
      id: "workspaces.createError",
      text: formatMessage({ id: "workspaces.createError" }),
      type: "error",
    });
  };

  return (
    <>
      {isEditMode ? (
        <Card withPadding className={styles.animate}>
          <Form<CreateCloudWorkspaceFormValues>
            defaultValues={{
              name: "",
            }}
            schema={CreateCloudWorkspaceFormValidationSchema}
            onSubmit={onSubmit}
            onSuccess={onSuccess}
            onError={onError}
          >
            <FormControl<CreateCloudWorkspaceFormValues>
              label={formatMessage({ id: "form.workspaceName" })}
              name="name"
              fieldType="input"
              type="text"
            />
            <FormSubmissionButtons
              submitKey="form.saveChanges"
              onCancelClickCallback={toggleMode}
              allowNonDirtyCancel
            />
          </Form>
        </Card>
      ) : (
        <Box>
          <Button
            onClick={toggleMode}
            variant="secondary"
            data-testid="workspaces.createNew"
            size="lg"
            icon={<Icon type="plus" />}
            className={styles.createButton}
          >
            <FormattedMessage id={isFirstWorkspace ? "workspaces.createFirst" : "workspaces.createNew"} />
          </Button>
        </Box>
      )}
    </>
  );
};
