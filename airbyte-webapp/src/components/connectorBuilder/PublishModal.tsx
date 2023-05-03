import { Form, Formik, useField } from "formik";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import * as yup from "yup";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Spinner } from "components/ui/Spinner";

import { DeclarativeComponentSchema } from "core/request/ConnectorManifest";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { useNotificationService } from "hooks/services/Notification";
import {
  useListVersions,
  usePublishProject,
  useReleaseNewVersion,
} from "services/connectorBuilder/ConnectorBuilderProjectsService";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderField } from "./Builder/BuilderField";
import { ConnectorImage } from "./ConnectorImage";
import styles from "./PublishModal.module.scss";

const NOTIFICATION_ID = "connectorBuilder.publish";

export const PublishModal: React.FC<{ onClose: () => void }> = ({ onClose }) => {
  const analyticsService = useAnalyticsService();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { projectId, lastValidJsonManifest, currentProject } = useConnectorBuilderFormState();
  const { data: versions, isLoading: isLoadingVersions } = useListVersions(currentProject);
  const { mutateAsync: sendPublishRequest } = usePublishProject();
  const { mutateAsync: sendNewVersionRequest } = useReleaseNewVersion();
  const [connectorNameField, , nameHelpers] = useField<string>("global.connectorName");

  const minVersion = versions && versions.length > 0 ? Math.max(...versions.map((version) => version.version)) + 1 : 1;

  const initialValues = useMemo(
    () => ({
      name: connectorNameField.value,
      description: "",
      useVersion: true,
      version: minVersion,
    }),
    [connectorNameField.value, minVersion]
  );

  const schema = useMemo(
    () =>
      yup.object().shape({
        name: yup.string().required("form.empty.error").max(256, "connectorBuilder.maxLength"),
        description: yup.string().max(256, "connectorBuilder.maxLength"),
        useVersion: yup.bool(),
        version: yup.number().min(minVersion),
      }),
    [minVersion]
  );

  if (isLoadingVersions) {
    return (
      <Modal
        size="sm"
        title={
          <FormattedMessage
            id={currentProject.sourceDefinitionId ? "connectorBuilder.releaseNewVersion" : "connectorBuilder.publish"}
          />
        }
        onClose={onClose}
      >
        <ModalBody>
          <FlexContainer justifyContent="center">
            <Spinner />
          </FlexContainer>
        </ModalBody>
      </Modal>
    );
  }

  return (
    <Formik
      initialValues={initialValues}
      validationSchema={schema}
      onSubmit={async (values) => {
        const manifest = lastValidJsonManifest as DeclarativeComponentSchema;
        unregisterNotificationById(NOTIFICATION_ID);
        try {
          if (currentProject.sourceDefinitionId) {
            await sendNewVersionRequest({
              manifest,
              description: values.description,
              sourceDefinitionId: currentProject.sourceDefinitionId,
              projectId: currentProject.id,
              useAsActiveVersion: values.useVersion,
              version: values.version,
            });
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.RELEASE_NEW_PROJECT_VERSION, {
              actionDescription: "User released a new version of a Connector Builder project",
              projectVersion: values.version,
              projectId: currentProject.id,
            });
          } else {
            await sendPublishRequest({
              manifest,
              name: values.name,
              description: values.description,
              projectId,
            });
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.PUBLISH_PROJECT, {
              actionDescription: "User published a Connector Builder project",
              projectId: currentProject.id,
            });
          }

          // push name change upstream so it's updated
          nameHelpers.setValue(values.name);

          registerNotification({
            id: NOTIFICATION_ID,
            text: (
              <FormattedMessage
                id="connectorBuilder.publishedMessage"
                values={{ name: values.name, version: values.version }}
              />
            ),
            type: "success",
          });
          onClose();
        } catch (e) {
          registerNotification({
            id: NOTIFICATION_ID,
            text: <FormattedMessage id="form.error" values={{ message: e.message }} />,
            type: "error",
          });
        }
      }}
    >
      {({ isValid, isSubmitting }) => (
        <Modal
          size="sm"
          title={
            <FormattedMessage
              id={currentProject.sourceDefinitionId ? "connectorBuilder.releaseNewVersion" : "connectorBuilder.publish"}
            />
          }
          wrapIn={Form}
          onClose={onClose}
        >
          <ModalBody>
            <FlexContainer alignItems="flex-start" gap="xl">
              <ConnectorImage />
              <FlexItem grow>
                <FlexContainer direction="column">
                  <BuilderField path="name" type="string" label="Connector name" />
                  <FlexContainer direction="row">
                    {currentProject.sourceDefinitionId && (
                      <div className={styles.versionInput}>
                        <BuilderField path="version" type="string" label="Version" disabled />
                      </div>
                    )}
                    <BuilderField path="description" type="textarea" label="Description" optional />
                  </FlexContainer>
                </FlexContainer>
              </FlexItem>
            </FlexContainer>
          </ModalBody>
          <ModalFooter>
            <FlexItem grow>
              <FlexContainer justifyContent="space-between">
                {currentProject.sourceDefinitionId && (
                  <BuilderField path="useVersion" type="boolean" label="Set as active version" />
                )}
                <FlexItem grow>
                  <FlexContainer justifyContent="flex-end">
                    <Button variant="secondary" type="reset" onClick={onClose}>
                      <FormattedMessage id="form.cancel" />
                    </Button>
                    <Button type="submit" disabled={!isValid} isLoading={isSubmitting}>
                      <FormattedMessage
                        id={
                          currentProject.sourceDefinitionId
                            ? "connectorBuilder.releaseNewVersion"
                            : "connectorBuilder.publish"
                        }
                      />
                    </Button>
                  </FlexContainer>
                </FlexItem>
              </FlexContainer>
            </FlexItem>
          </ModalFooter>
        </Modal>
      )}
    </Formik>
  );
};
