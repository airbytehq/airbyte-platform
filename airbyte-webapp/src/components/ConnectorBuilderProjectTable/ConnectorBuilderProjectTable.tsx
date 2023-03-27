import { faTrashCan, faPenToSquare } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { SortOrderEnum } from "components/EntityTable/types";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Table } from "components/ui/Table";
import { SortableTableHeader } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { ToastType } from "components/ui/Toast";
import { Tooltip } from "components/ui/Tooltip";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { getEditPath } from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { BuilderProject, useDeleteProject } from "services/connectorBuilder/ConnectorBuilderProjectsService";

import styles from "./ConnectorBuilderProjectTable.module.scss";
import { DefaultLogoCatalog } from "./DefaultLogoCatalog";

const columnHelper = createColumnHelper<BuilderProject>();

const FAILED_TO_DELETE_PROJECT_ID = "connectorBuilder.deleteProject.error";

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
  const { registerNotification } = useNotificationService();
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
            <DefaultLogoCatalog />
            <Text>{props.cell.getValue()}</Text>
          </FlexContainer>
        ),
      }),
      columnHelper.accessor("version", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.version" />,
        cell: (props) => {
          const value = props.getValue();
          return (
            <Tooltip
              disabled={value !== "draft"}
              control={
                <Text className={classNames(styles.versionCell, { [styles.draft]: value === "draft" })}>
                  {value === "draft" ? <FormattedMessage id="connectorBuilder.draft" /> : `v${value}`}
                </Text>
              }
            >
              <FormattedMessage id="connectorBuilder.listPage.draftTooltip" />
            </Tooltip>
          );
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
                    openConfirmationModal({
                      text: "connectorBuilder.deleteProjectModal.text",
                      title: "connectorBuilder.deleteProjectModal.title",
                      submitButtonText: "connectorBuilder.deleteProjectModal.submitButton",
                      onSubmit: () => {
                        closeConfirmationModal();
                        deleteProject(props.row.original).catch((e) => {
                          registerNotification({
                            id: FAILED_TO_DELETE_PROJECT_ID,
                            text: (
                              <FormattedMessage
                                id={FAILED_TO_DELETE_PROJECT_ID}
                                values={{
                                  reason: e.message,
                                }}
                              />
                            ),
                            type: ToastType.ERROR,
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
      basePath,
      closeConfirmationModal,
      deleteProject,
      navigate,
      onSortClick,
      openConfirmationModal,
      registerNotification,
      sortBy,
      sortOrder,
    ]
  );

  return <Table columns={columns} data={projects} className={styles.table} />;
};
