/**
 * This should not be used in cloud until:
 * - all cloud users have an org
 * - all cloud users have permissions in configdb
 * - we are able to use the oss create workspace endpoint in cloud

 */

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
import { Text } from "components/ui/Text";

import { useListWorkspaces } from "core/api";
import { OrganizationRead, WorkspaceCreate, WorkspaceRead } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";

import { useOrganizationsToCreateWorkspaces } from "./useOrganizationsToCreateWorkspaces";
import styles from "./WorkspacesCreateControl.module.scss";

interface CreateWorkspaceFormValues {
  name: string;
  organizationId: string;
}

const OrganizationCreateWorkspaceFormValidationSchema = yup.object().shape({
  name: yup.string().trim().required("form.empty.error"),
  organizationId: yup.string().trim().required("form.empty.error"),
});

interface OrganizationWorkspacesCreateControlProps {
  createWorkspace: UseMutateAsyncFunction<WorkspaceRead, unknown, WorkspaceCreate, unknown>;
}

export const WorkspacesCreateControl: React.FC<OrganizationWorkspacesCreateControlProps> = ({ createWorkspace }) => {
  const { organizationsToCreateIn } = useOrganizationsToCreateWorkspaces();

  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const [isEditMode, toggleMode] = useToggle(false);
  const { registerNotification } = useNotificationService();
  const { workspaces } = useListWorkspaces();
  const isFirstWorkspace = workspaces.length === 0;

  // if the user does not have create permissions in any organizations, do not show the control at all
  if (organizationsToCreateIn.length === 0) {
    return null;
  }

  const onSubmit = async (values: CreateWorkspaceFormValues) => {
    const newWorkspace = await createWorkspace(values);
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

  const onError = (e: Error, { name }: CreateWorkspaceFormValues) => {
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
          <Form<CreateWorkspaceFormValues>
            defaultValues={{
              name: "",
              organizationId: organizationsToCreateIn[0].organizationId,
            }}
            schema={OrganizationCreateWorkspaceFormValidationSchema}
            onSubmit={onSubmit}
            onSuccess={onSuccess}
            onError={onError}
          >
            <WorkspaceCreateControlFormContent organizations={organizationsToCreateIn} toggleMode={toggleMode} />
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

const WorkspaceCreateControlFormContent: React.FC<{ organizations: OrganizationRead[]; toggleMode: () => void }> = ({
  organizations,
  toggleMode,
}) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {organizations.length > 1 && (
        <FormControl<CreateWorkspaceFormValues>
          label={formatMessage({ id: "form.organizationName" })}
          name="organizationId"
          fieldType="dropdown"
          options={organizations.map((organization) => {
            return {
              value: organization.organizationId,
              label: organization.organizationName,
            };
          })}
        />
      )}
      <>
        <FormControl<CreateWorkspaceFormValues>
          label={formatMessage({ id: "form.workspaceName" })}
          name="name"
          fieldType="input"
          type="text"
        />
        {organizations.length === 1 && (
          <Box pb="md">
            <Text>
              <FormattedMessage
                id="workspaces.create.organizationDescription"
                values={{ organizationName: organizations[0].organizationName }}
              />
            </Text>
          </Box>
        )}
        <FormSubmissionButtons submitKey="form.saveChanges" onCancelClickCallback={toggleMode} allowNonDirtyCancel />
      </>
    </>
  );
};
