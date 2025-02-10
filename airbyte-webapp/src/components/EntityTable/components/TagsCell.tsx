import { CellContext, ColumnDefTemplate } from "@tanstack/react-table";
import { useCallback, useEffect, useState } from "react";

import { SelectConnectionTags } from "components/connection/SelectConnectionTags/SelectConnectionTags";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { TagBadge } from "components/ui/TagBadge";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateTag, useTagsList, useUpdateConnectionOptimistically } from "core/api";
import { Tag } from "core/api/types/AirbyteClient";

import { ConnectionTableDataItem } from "../types";

export const TagsCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, Tag[]>> = (props) => {
  const workspaceId = useCurrentWorkspaceId();
  const availableTags = useTagsList(workspaceId);
  const { mutateAsync: createTag } = useCreateTag();
  const initialValue = props.getValue();
  const [selectedTags, setSelectedTags] = useState(props.getValue());
  const { mutateAsync: updateConnectionTags } = useUpdateConnectionOptimistically();

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
    updateConnectionTags({
      connectionId: props.row.original.connectionId,
      tags: selectedTags,
    });
  }, [props.row.original.connectionId, selectedTags, updateConnectionTags]);

  return (
    <Box py="md" px="lg">
      <FlexContainer gap="sm" alignItems="center" wrap="wrap">
        {selectedTags?.map((tag) => <TagBadge color={tag.color} key={tag.tagId} text={tag.name} />)}
        <SelectConnectionTags
          availableTags={availableTags}
          selectedTags={selectedTags}
          createTag={onCreateTag}
          selectTag={onTagSelect}
          deselectTag={onTagDeselect}
          onClose={updateTags}
        />
      </FlexContainer>
    </Box>
  );
};
