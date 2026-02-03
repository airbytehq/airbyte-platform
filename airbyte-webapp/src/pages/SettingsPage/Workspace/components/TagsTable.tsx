import { createColumnHelper } from "@tanstack/react-table";
import React, { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Table } from "components/ui/Table";
import { TagBadge } from "components/ui/TagBadge";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useDeleteTag, useTagsList } from "core/api";
import { Tag } from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { TagFormModal } from "./TagFormModal";
import styles from "./TagsTable.module.scss";

export const TagsTable: React.FC = () => {
  const { workspaceId } = useCurrentWorkspace();
  const tags = useTagsList(workspaceId);
  const { mutateAsync: deleteTag } = useDeleteTag();
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const canEditTags = useGeneratedIntent(Intent.CreateOrEditConnection);

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const onEdit = useCallback(
    (tag?: Tag) => {
      openModal({
        title: formatMessage({
          id: tag ? "settings.workspace.tags.tagForm.edit" : "settings.workspace.tags.tagForm.create",
        }),
        content: ({ onCancel, onComplete }) => (
          <TagFormModal tag={tag} onCancel={onCancel} onComplete={() => onComplete({ type: "completed" })} />
        ),
      });
    },
    [openModal, formatMessage]
  );

  const onDelete = useCallback(
    (tag?: Tag) => {
      if (!tag) {
        return;
      }
      openConfirmationModal({
        title: "settings.workspace.tags.deleteModal.title",
        text: (
          <FormattedMessage
            id="settings.workspace.tags.deleteModal.text"
            values={{
              tag: tag.name,
            }}
          />
        ),
        submitButtonText: "settings.workspace.tags.deleteModal.submit",
        onSubmit: async () => {
          await deleteTag({ tagId: tag.tagId, workspaceId });
          closeConfirmationModal();
          registerNotification({
            id: "settings.workspace.tags.tagDeleteSuccess",
            text: formatMessage({ id: "settings.workspace.tags.tagDeleteSuccess" }),
            type: "success",
          });
        },
        onCancel: () => {
          closeConfirmationModal();
        },
      });
    },
    [closeConfirmationModal, deleteTag, formatMessage, openConfirmationModal, registerNotification, workspaceId]
  );

  const columnHelper = createColumnHelper<Tag>();

  const columns = React.useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="settings.workspace.tags.tagColumn" />,
        cell: (props) => <TagBadge color={props.row.original.color} text={props.row.original.name} />,
      }),
      columnHelper.display({
        id: "actions",
        header: () => "",
        cell: (props) => (
          <FlexContainer gap="sm">
            <Button
              variant="clear"
              icon="pencil"
              size="xs"
              aria-label="Edit"
              onClick={() => onEdit(props.row.original)}
              disabled={!canEditTags}
            />
            <Button
              variant="clear"
              icon="trash"
              size="xs"
              aria-label="Delete"
              onClick={() => onDelete(props.row.original)}
              disabled={!canEditTags}
            />
          </FlexContainer>
        ),
        meta: {
          thClassName: styles.actions,
          tdClassName: styles.actions,
          noPadding: true,
        },
      }),
    ],
    [columnHelper, onDelete, onEdit, canEditTags]
  );

  return (
    <Box>
      <FlexContainer direction="column">
        <FlexContainer justifyContent="space-between" alignItems="flex-start">
          <FlexContainer direction="column" gap="sm">
            <Heading as="h3" size="sm">
              <FormattedMessage id="settings.workspace.tags.title" />
            </Heading>
            <Text>
              <FormattedMessage id="settings.workspace.tags.description" />
            </Text>
          </FlexContainer>
          <Button
            variant="primary"
            size="xs"
            icon="plus"
            aria-label="create tag"
            onClick={() => onEdit()}
            disabled={!canEditTags}
          >
            <FormattedMessage id="settings.workspace.tags.tagForm.create" />
          </Button>
        </FlexContainer>
        <Table columns={columns} data={tags} initialSortBy={[{ id: "name", desc: false }]} />
      </FlexContainer>
    </Box>
  );
};
