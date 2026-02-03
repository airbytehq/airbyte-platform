import { CellContext, ColumnDefTemplate } from "@tanstack/react-table";
import { useCallback, useEffect, useState } from "react";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { TagBadge } from "components/ui/TagBadge";

import { SelectConnectionTags } from "area/connection/components/SelectConnectionTags/SelectConnectionTags";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateTag, useTagsList, useUpdateConnectionOptimistically } from "core/api";
import { Tag } from "core/api/types/AirbyteClient";

import styles from "./TagsCell.module.scss";
import { ConnectionTableDataItem } from "../types";

export const TagsCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, Tag[]>> = (props) => {
  const workspaceId = useCurrentWorkspaceId();
  const availableTags = useTagsList(workspaceId);
  const { mutateAsync: createTag } = useCreateTag();
  const initialValue = props.getValue();
  const [selectedTags, setSelectedTags] = useState(props.getValue());
  const { mutateAsync: updateConnectionOptimistically } = useUpdateConnectionOptimistically();

  useEffect(() => {
    setSelectedTags(initialValue);
  }, [initialValue]);

  const onCreateTag = async (name: string, color: string) => {
    const newTag = await createTag({ name, color, workspaceId });
    setSelectedTags((prev) => [...(prev || []), newTag]);
  };

  const onTagSelect = (selectedTag: Tag) => {
    setSelectedTags((prev) => [...(prev || []), selectedTag]);
  };

  const onTagDeselect = (deselectedTag: Tag) => {
    setSelectedTags((prev) => (prev || []).filter((tag) => tag.tagId !== deselectedTag.tagId));
  };

  const updateTags = useCallback(() => {
    updateConnectionOptimistically({
      connectionId: props.row.original.connectionId,
      tags: selectedTags,
      skipReset: true,
    });
  }, [props.row.original.connectionId, selectedTags, updateConnectionOptimistically]);

  return (
    <Box py="md" px="lg">
      <FlexContainer gap="sm" alignItems="center" wrap="wrap">
        {selectedTags?.map((tag) => <TagBadge color={tag.color} key={tag.tagId} text={tag.name} />)}
        <span className={styles.tagsCell__selectButton}>
          <SelectConnectionTags
            availableTags={availableTags}
            selectedTags={selectedTags}
            createTag={onCreateTag}
            selectTag={onTagSelect}
            deselectTag={onTagDeselect}
            onClose={updateTags}
          />
        </span>
      </FlexContainer>
    </Box>
  );
};
