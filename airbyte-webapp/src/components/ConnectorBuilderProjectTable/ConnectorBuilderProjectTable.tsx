import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useCallback, useMemo, useState } from "react";
import { FormattedDate, FormattedMessage, useIntl } from "react-intl";

import { BaseConnectorInfo } from "components/connectorBuilder/BaseConnectorInfo";
import { ConnectorIcon } from "components/ConnectorIcon";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink, Link } from "components/ui/Link";
import { ModalBody } from "components/ui/Modal";
import { Spinner } from "components/ui/Spinner";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import {
  BuilderProject,
  useChangeBuilderProjectVersion,
  useDeleteBuilderProject,
  useListBuilderProjectVersions,
  useSourceDefinitionList,
} from "core/api";
import { ContributionInfo } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
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
  const canUpdateConnector = useGeneratedIntent(Intent.CreateOrEditConnectorBuilder);
  const getEditUrl = useCallback(
    (projectId: string) => `${basePath ? basePath : ""}${getEditPath(projectId)}`,
    [basePath]
  );

  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.name" />,
        cell: (props) => (
          <FlexContainer direction="column" gap="none">
            <Link to={getEditUrl(props.row.original.id)} className={styles.nameLink}>
              {/* TODO: replace with custom logos once available */}
              <BuilderLogo />
              <Text>{props.cell.getValue()}</Text>
              {props.row.original.baseActorDefinitionVersionInfo && (
                <BaseConnectorInfo
                  className={styles.baseConnectorInfo}
                  disableTooltip
                  {...props.row.original.baseActorDefinitionVersionInfo}
                />
              )}
            </Link>
            {props.row.original.contributionInfo && (
              <ContributionInfoDisplay {...props.row.original.contributionInfo} />
            )}
          </FlexContainer>
        ),
        meta: {
          tdClassName: styles.nameCell,
        },
      }),
      columnHelper.accessor("version", {
        enableSorting: false,
        meta: {
          thClassName: styles.versionHeader,
        },
        header: () => <FormattedMessage id="connectorBuilder.listPage.version" />,
        cell: (props) => {
          return (
            <BuilderCell>
              <VersionChanger canUpdateConnector={canUpdateConnector} project={props.row.original} />
            </BuilderCell>
          );
        },
      }),
      columnHelper.accessor("updatedAt", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.updatedAt" />,
        cell: (props) => {
          return (
            <BuilderCell className={styles.updatedAtCell}>
              <FormattedDate value={props.getValue() * 1000} timeStyle="short" dateStyle="medium" />
            </BuilderCell>
          );
        },
      }),
      columnHelper.display({
        id: "actions",
        header: () => null,
        cell: (props) => (
          <BuilderCell>
            <FlexContainer justifyContent="flex-end" gap="sm" alignItems="center" className={styles.actions}>
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
                        data-testid={`delete-project-button_${props.row.original.name}`}
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
          </BuilderCell>
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
      rowId="id"
      columns={columns}
      data={projects}
      className={styles.table}
      sorting
      stickyHeaders={false}
      initialSortBy={[{ id: "name", desc: false }]}
    />
  );
};

const BuilderCell: React.FC<React.PropsWithChildren<{ className?: string }>> = ({ children, className }) => {
  return (
    <FlexContainer className={classNames(styles.cell, className)} alignItems="center">
      {children}
    </FlexContainer>
  );
};

const ContributionInfoDisplay: React.FC<ContributionInfo> = ({ actorDefinitionId, pullRequestUrl }) => {
  // list instead of fetching definition individually to reuse cached request and avoid 404 for net-new definitions
  const sourceDefinition = useSourceDefinitionList().sourceDefinitionMap.get(actorDefinitionId);

  return (
    <FlexContainer direction="row" className={styles.contributionInfo} gap="sm">
      <Icon className={styles.contributionIcon} type="nested" color="action" />
      <FlexContainer className={styles.contributionText} gap="sm" alignItems="center">
        <Text color="grey" size="sm">
          <FormattedMessage
            id={
              sourceDefinition?.name
                ? "connectorBuilder.listPage.contributing.toExising"
                : "connectorBuilder.listPage.contributing.asNew"
            }
            values={{
              name: sourceDefinition?.name,
            }}
          />
        </Text>
        {sourceDefinition?.icon ? (
          <ConnectorIcon icon={sourceDefinition.icon} className={styles.connectorIcon} />
        ) : undefined}
        <ExternalLink href={pullRequestUrl}>
          <FormattedMessage
            id="connectorBuilder.listPage.contributing.pullRequest"
            values={{ prNumber: pullRequestUrl.split("/").pop() }}
          />
        </ExternalLink>
      </FlexContainer>
    </FlexContainer>
  );
};
