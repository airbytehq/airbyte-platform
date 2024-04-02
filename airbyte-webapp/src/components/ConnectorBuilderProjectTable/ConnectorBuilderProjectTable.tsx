import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useCallback, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { ModalBody } from "components/ui/Modal";
import { Spinner } from "components/ui/Spinner";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import {
  BuilderProject,
  useChangeBuilderProjectVersion,
  useCurrentWorkspace,
  useDeleteBuilderProject,
  useListBuilderProjectVersions,
} from "core/api";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useIntent } from "core/utils/rbac";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useModalService } from "hooks/services/Modal";
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
  onComplete: () => void;
}> = ({ onComplete, project }) => {
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
      onComplete();
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
    <ModalBody>
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
  );
};

const VersionChanger = ({ project, canUpdateConnector }: { project: BuilderProject; canUpdateConnector: boolean }) => {
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();

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

  const openVersionChangeModal = () =>
    openModal<void>({
      title: formatMessage({ id: "connectorBuilder.changeVersionModal.title" }, { name: project.name }),
      size: "sm",
      content: ({ onComplete }) => <VersionChangeModal project={project} onComplete={onComplete} />,
    });

  return (
    <Button
      disabled={!canUpdateConnector}
      variant="clear"
      onClick={openVersionChangeModal}
      icon="chevronDown"
      iconPosition="right"
      data-testid={`version-changer-${project.name}`}
    >
      <Text>v{project.version}</Text>
    </Button>
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
  const { workspaceId } = useCurrentWorkspace();
  const canUpdateConnector = useIntent("UpdateCustomConnector", { workspaceId });
  const getEditUrl = useCallback(
    (projectId: string) => `${basePath ? basePath : ""}${getEditPath(projectId)}`,
    [basePath]
  );
  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.name" />,
        cell: (props) => (
          <Link to={getEditUrl(props.row.original.id)} className={styles.nameLink}>
            {/* TODO: replace with custom logos once available */}
            <BuilderLogo />
            <Text>{props.cell.getValue()}</Text>
          </Link>
        ),
        meta: {
          tdClassName: styles.nameCell,
        },
      }),
      columnHelper.accessor("version", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.version" />,
        cell: (props) => {
          return <VersionChanger canUpdateConnector={canUpdateConnector} project={props.row.original} />;
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
            {canUpdateConnector ? (
              <>
                <Link
                  to={getEditUrl(props.row.original.id)}
                  data-testid={`edit-project-button-${props.row.original.name}`}
                >
                  <Icon type="pencil" />
                </Link>
                <Tooltip
                  disabled={!props.row.original.sourceDefinitionId}
                  control={
                    <Button
                      type="button"
                      variant="clear"
                      disabled={Boolean(props.row.original.sourceDefinitionId)}
                      icon="trash"
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
              </>
            ) : (
              <Link
                to={getEditUrl(props.row.original.id)}
                data-testid={`view-project-button-${props.row.original.name}`}
              >
                <Icon type="eye" />
              </Link>
            )}
          </FlexContainer>
        ),
      }),
    ],
    [
      getEditUrl,
      canUpdateConnector,
      unregisterNotificationById,
      openConfirmationModal,
      closeConfirmationModal,
      deleteProject,
      analyticsService,
      registerNotification,
    ]
  );

  return (
    <Table
      columns={columns}
      data={projects}
      className={styles.table}
      sorting={false}
      stickyHeaders={false}
      initialSortBy={[{ id: "name", desc: false }]}
    />
  );
};
