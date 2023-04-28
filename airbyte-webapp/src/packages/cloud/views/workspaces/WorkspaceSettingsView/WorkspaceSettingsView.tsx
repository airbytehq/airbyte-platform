import classNames from "classnames";
import { Field, FieldProps, Form, Formik } from "formik";
import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import * as yup from "yup";

import { LabeledInput } from "components";
import { Button } from "components/ui/Button";

import { useTrackPage, PageTrackingCodes } from "hooks/services/Analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import {
  useRemoveCloudWorkspace,
  useUpdateCloudWorkspace,
} from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { RoutePaths } from "pages/routePaths";
import { Content, SettingsCard } from "pages/SettingsPage/pages/SettingsComponents";
import { useInvalidateWorkspace, useWorkspaceService } from "services/workspaces/WorkspacesService";

import styles from "./WorkspaceSettingsView.module.scss";

const ValidationSchema = yup.object().shape({
  name: yup.string().required("form.empty.error"),
});

export const WorkspaceSettingsView: React.FC = () => {
  const { formatMessage } = useIntl();
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE);
  const { exitWorkspace } = useWorkspaceService();
  const workspace = useCurrentWorkspace();
  const { mutateAsync: removeCloudWorkspace, isLoading: isRemovingCloudWorkspace } = useRemoveCloudWorkspace();
  const { mutateAsync: updateCloudWorkspace } = useUpdateCloudWorkspace();
  const invalidateWorkspace = useInvalidateWorkspace(workspace.workspaceId);
  const { registerNotification } = useNotificationService();
  const [workspaceWasDeleted, setWorkspaceWasDeleted] = useState(false);
  const navigate = useNavigate();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const deleteCurrentWorkspace = () => {
    openConfirmationModal({
      title: "settings.workspaceSettings.delete.confirmation.title",
      text: "settings.workspaceSettings.delete.confirmation.text",
      submitButtonText: "settings.workspaceSettings.delete.confirmation.submitButtonText",
      onSubmit() {
        removeCloudWorkspace(workspace.workspaceId)
          .then(() => {
            registerNotification({
              id: "settings.workspace.delete.success",
              text: formatMessage({ id: "settings.workspaceSettings.delete.success" }),
              type: "success",
            });
            setWorkspaceWasDeleted(true);
            setTimeout(() => navigate(`/${RoutePaths.Workspaces}`), 600);
          })
          .catch(() => {
            registerNotification({
              id: "settings.workspace.delete.error",
              text: formatMessage({ id: "settings.workspaceSettings.delete.error" }),
              type: "error",
            });
          })
          .finally(closeConfirmationModal);
      },
    });
  };

  return (
    <>
      <SettingsCard
        title={
          <div className={styles.header}>
            <FormattedMessage id="settings.generalSettings" />
            <Button type="button" onClick={exitWorkspace} data-testid="button.changeWorkspace">
              <FormattedMessage id="settings.generalSettings.changeWorkspace" />
            </Button>
          </div>
        }
      >
        <Formik
          initialValues={{
            name: workspace.name,
          }}
          onSubmit={async (payload) => {
            const { workspaceId } = workspace;
            await updateCloudWorkspace({
              workspaceId,
              name: payload.name,
            });
            await invalidateWorkspace();
          }}
          enableReinitialize
          validationSchema={ValidationSchema}
        >
          {({ dirty, isSubmitting, resetForm, isValid }) => (
            <Form>
              <Content>
                <Field name="name">
                  {({ field, meta }: FieldProps<string>) => (
                    <LabeledInput
                      {...field}
                      label={<FormattedMessage id="settings.generalSettings.form.name.label" />}
                      placeholder={formatMessage({
                        id: "settings.generalSettings.form.name.placeholder",
                      })}
                      type="text"
                      error={!!meta.error && meta.touched}
                      message={meta.touched && meta.error && formatMessage({ id: meta.error })}
                    />
                  )}
                </Field>

                <div className={classNames(styles.formItem, styles.buttonGroup)}>
                  <Button type="button" variant="secondary" disabled={!dirty} onClick={() => resetForm()}>
                    <FormattedMessage id="form.cancel" />
                  </Button>
                  <Button type="submit" disabled={!dirty || !isValid} isLoading={isSubmitting}>
                    <FormattedMessage id="form.saveChanges" />
                  </Button>
                </div>
              </Content>
            </Form>
          )}
        </Formik>
      </SettingsCard>
      <SettingsCard
        title={
          <div className={styles.header}>
            <FormattedMessage id="settings.generalSettings.deleteLabel" />
            <Button
              isLoading={isRemovingCloudWorkspace}
              variant="danger"
              onClick={deleteCurrentWorkspace}
              disabled={workspaceWasDeleted}
            >
              <FormattedMessage id="settings.generalSettings.deleteText" />
            </Button>
          </div>
        }
      />
    </>
  );
};
