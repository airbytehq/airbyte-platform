import { faTrashCan, faPenToSquare, faCaretDown } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { SortOrderEnum } from "components/EntityTable/types";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Modal, ModalBody } from "components/ui/Modal";
import { Spinner } from "components/ui/Spinner";
import { Table } from "components/ui/Table";
import { SortableTableHeader } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { Action, Namespace } from "core/analytics";
import { useAnalyticsService } from "hooks/services/Analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { getEditPath } from "pages/connectorBuilder/ConnectorBuilderRoutes";
import {
  BuilderProject,
  useChangeVersion,
  useDeleteProject,
  useListVersions,
} from "services/connectorBuilder/ConnectorBuilderProjectsService";

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
  const { data: versions, isLoading: isLoadingVersionList } = useListVersions(project);
  const [selectedVersion, setSelectedVersion] = useState<number | undefined>(undefined);
  const { mutateAsync: changeVersion, isLoading } = useChangeVersion();
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
          <Button variant="clear" disabled>
            <Text className={classNames(styles.versionCell, styles.draft)}>
              <FormattedMessage id="connectorBuilder.draft" />
            </Text>
          </Button>
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
        icon={<FontAwesomeIcon icon={faCaretDown} />}
        iconPosition="right"
        data-testid={`version-changer-${project.name}`}
      >
        <Text className={styles.versionCell}>v{project.version}</Text>
      </Button>
      {changeInProgress && <VersionChangeModal onClose={() => setChangeInProgress(false)} project={project} />}
    </>
  );
};

export const ConnectorBuilderProjectTable = ({
  projects,
  onSortClick,
  sortBy,
  sortOrder,
  basePath,
}: {
  projects: BuilderProject[];
  onSortClick?: (field: string) => void;
  sortBy?: string;
  sortOrder?: SortOrderEnum;
  basePath?: string;
}) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const analyticsService = useAnalyticsService();
  const { mutateAsync: deleteProject } = useDeleteProject();
  const navigate = useNavigate();
  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () =>
          onSortClick ? (
            <SortableTableHeader
              onClick={() => onSortClick("name")}
              isActive={sortBy === "name"}
              isAscending={sortOrder === SortOrderEnum.ASC}
            >
              <FormattedMessage id="connectorBuilder.listPage.name" />
            </SortableTableHeader>
          ) : (
            <FormattedMessage id="connectorBuilder.listPage.name" />
          ),
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
        meta: {
          responsive: true,
        },
        cell: (props) => (
          <FlexContainer justifyContent="flex-end" gap="sm">
            <Button
              variant="clear"
              data-testid={`edit-project-button-${props.row.original.name}`}
              icon={<FontAwesomeIcon icon={faPenToSquare} />}
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
                  icon={<FontAwesomeIcon icon={faTrashCan} />}
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
      onSortClick,
      openConfirmationModal,
      registerNotification,
      sortBy,
      sortOrder,
      unregisterNotificationById,
    ]
  );

  return <Table columns={columns} data={projects} className={styles.table} />;
};
