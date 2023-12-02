import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Modal, ModalBody } from "components/ui/Modal";
import { Spinner } from "components/ui/Spinner";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import {
  BuilderProject,
  useChangeBuilderProjectVersion,
  useDeleteBuilderProject,
  useListBuilderProjectVersions,
} from "core/api";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { getEditPath } from "pages/connectorBuilder/ConnectorBuilderRoutes";

import styles from "./ConnectorBuilderProjectTable.module.scss";
import { BuilderLogo } from "../connectorBuilder/BuilderLogo";

const columnHelper = createColumnHelper<BuilderProject>();

const NOTIFICATION_ID = "connectorBuilder.manageProjects.notification";

const DELETE_PROJECT_ERROR_ID = "connectorBuilder.deleteProject.error";
const CHANGED_VERSION_SUCCESS_ID = "connectorBuilder.changeVersion.success";
const CHANGED_VERSION_ERROR_ID = "connectorBuilder.changeVersion.error";

const VersionChangeModal: React.FC<{
  project: BuilderProject;
  onClose: () => void;
}> = ({ onClose, project }) => {
  const { data: versions, isLoading: isLoadingVersionList } = useListBuilderProjectVersions(project);
  const [selectedVersion, setSelectedVersion] = useState<number | undefined>(undefined);
  const { mutateAsync: changeVersion, isLoading } = useChangeBuilderProjectVersion();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const analyticsService = useAnalyticsService();

  async function onSelect(version: number) {
    if (!project.sourceDefinitionId) {
      return;
    }
    unregisterNotificationById(NOTIFICATION_ID);
    try {
      setSelectedVersion(version);
      await changeVersion({ sourceDefinitionId: project.sourceDefinitionId, builderProjectId: project.id, version });
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.CHANGE_PROJECT_VERSION, {
        actionDescription: "User selected an active version for a Connector Builder project",
        projectId: project.id,
        projectVersion: version,
      });
      registerNotification({
        id: NOTIFICATION_ID,
        text: (
          <FormattedMessage
            id={CHANGED_VERSION_SUCCESS_ID}
            values={{
              name: project.name,
              oldVersion: project.version,
              newVersion: version,
            }}
          />
        ),
        type: "success",
      });
      onClose();
    } catch (e) {
      registerNotification({
        id: NOTIFICATION_ID,
        text: (
          <FormattedMessage
            id={CHANGED_VERSION_ERROR_ID}
            values={{
              reason: e.message,
              name: project.name,
            }}
          />
        ),
        type: "success",
      });
    }
  }

  return (
    <Modal
      size="sm"
      title={<FormattedMessage id="connectorBuilder.changeVersionModal.title" values={{ name: project.name }} />}
      onClose={onClose}
    >
      <ModalBody className={styles.modalStreamListContainer}>
        {isLoadingVersionList ? (
          <FlexContainer justifyContent="center">
            <Spinner size="md" />
          </FlexContainer>
        ) : (
          <FlexContainer direction="column" data-testid="versions-list">
            {(versions || []).map((version, index) => (
              <button
                key={index}
                onClick={() => {
                  onSelect(version.version);
                }}
                className={classNames(styles.versionItem, { [styles["versionItem--active"]]: version.isActive })}
              >
                <FlexContainer alignItems="baseline">
                  <Text size="md" as="span">
                    v{version.version}{" "}
                  </Text>
                  <FlexItem grow>
                    <Text size="sm" as="span" color="grey">
                      {version.description}
                    </Text>
                  </FlexItem>
                  {version.isActive && (
                    <Text className={styles.activeVersion} size="xs" as="span">
                      <FormattedMessage id="connectorBuilder.changeVersionModal.activeVersion" />
                    </Text>
                  )}
                  {isLoading && version.version === selectedVersion && <Spinner size="xs" />}
                </FlexContainer>
              </button>
            ))}
          </FlexContainer>
        )}
      </ModalBody>
    </Modal>
  );
};

const VersionChanger = ({ project }: { project: BuilderProject }) => {
  const [changeInProgress, setChangeInProgress] = useState(false);
  if (project.version === "draft") {
    return (
      <Tooltip
        control={
          <Text color="grey" className={styles.draft}>
            <FormattedMessage id="connectorBuilder.draft" />
          </Text>
        }
      >
        <FormattedMessage id="connectorBuilder.listPage.draftTooltip" />
      </Tooltip>
    );
  }
  return (
    <>
      <Button
        variant="clear"
        onClick={() => {
          setChangeInProgress(true);
        }}
        icon={<Icon type="chevronDown" />}
        iconPosition="right"
        data-testid={`version-changer-${project.name}`}
      >
        <Text>v{project.version}</Text>
      </Button>
      {changeInProgress && <VersionChangeModal onClose={() => setChangeInProgress(false)} project={project} />}
    </>
  );
};

export const ConnectorBuilderProjectTable = ({
  projects,
  basePath,
}: {
  projects: BuilderProject[];
  basePath?: string;
}) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const analyticsService = useAnalyticsService();
  const { mutateAsync: deleteProject } = useDeleteBuilderProject();
  const navigate = useNavigate();
  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.name" />,
        cell: (props) => (
          <FlexContainer alignItems="center">
            {/* TODO: replace with custom logos once available */}
            <BuilderLogo />
            <Text>{props.cell.getValue()}</Text>
          </FlexContainer>
        ),
      }),
      columnHelper.accessor("version", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.version" />,
        cell: (props) => {
          return <VersionChanger project={props.row.original} />;
        },
      }),
      columnHelper.display({
        id: "actions",
        header: () => null,
        cell: (props) => (
          <FlexContainer justifyContent="flex-end" gap="sm" alignItems="center">
            <Text className={styles.draftInProgress} color="grey">
              {props.row.original.hasDraft && <FormattedMessage id="connectorBuilder.draftInProgressLabel" />}
            </Text>
            <Button
              variant="clear"
              data-testid={`edit-project-button-${props.row.original.name}`}
              icon={<Icon type="pencil" />}
              onClick={() => {
                const editPath = getEditPath(props.row.original.id);
                navigate(basePath ? `${basePath}${editPath}` : editPath);
              }}
            />
            <Tooltip
              disabled={!props.row.original.sourceDefinitionId}
              control={
                <Button
                  type="button"
                  variant="clear"
                  disabled={Boolean(props.row.original.sourceDefinitionId)}
                  icon={<Icon type="trash" />}
                  onClick={() => {
                    unregisterNotificationById(NOTIFICATION_ID);
                    openConfirmationModal({
                      text: "connectorBuilder.deleteProjectModal.text",
                      title: "connectorBuilder.deleteProjectModal.title",
                      submitButtonText: "connectorBuilder.deleteProjectModal.submitButton",
                      onSubmit: () => {
                        closeConfirmationModal();
                        deleteProject(props.row.original)
                          .then(() => {
                            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.CONNECTOR_BUILDER_DELETE, {
                              actionDescription: "User has deleted a Connector Builder project",
                              projectId: props.row.original.id,
                            });
                          })
                          .catch((e) => {
                            registerNotification({
                              id: NOTIFICATION_ID,
                              text: (
                                <FormattedMessage
                                  id={DELETE_PROJECT_ERROR_ID}
                                  values={{
                                    reason: e.message,
                                  }}
                                />
                              ),
                              type: "error",
                            });
                          });
                      },
                    });
                  }}
                />
              }
            >
              <FormattedMessage id="connectorBuilder.deleteProject.publishedTooltip" />
            </Tooltip>
          </FlexContainer>
        ),
      }),
    ],
    [
      analyticsService,
      basePath,
      closeConfirmationModal,
      deleteProject,
      navigate,
      openConfirmationModal,
      registerNotification,
      unregisterNotificationById,
    ]
  );

  return (
    <Table
      columns={columns}
      data={projects}
      className={styles.table}
      sorting={false}
      initialSortBy={[{ id: "name", desc: false }]}
    />
  );
};
