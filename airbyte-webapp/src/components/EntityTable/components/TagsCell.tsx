import { CellContext, ColumnDefTemplate } from "@tanstack/react-table";

import { SelectConnectionTags } from "components/connection/SelectConnectionTags/SelectConnectionTags";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { TagBadge } from "components/ui/TagBadge";

import { Tag } from "core/api/types/AirbyteClient";

import { ConnectionTableDataItem } from "../types";

export const TagsCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, Tag[]>> = (props) => {
  if (!props.getValue()) {
    return null;
  }

  return (
    <Box py="md" px="lg">
      <FlexContainer gap="sm" alignItems="center">
        {props.getValue()?.map((tag) => <TagBadge color={tag.color} key={tag.tagId} text={tag.name} />)}
        <SelectConnectionTags
          selectedTags={props.getValue()}
          availableTags={[]}
          createTag={() => {}}
          selectTag={() => {}}
          deselectTag={() => {}}
        />
      </FlexContainer>
    </Box>
  );
};
