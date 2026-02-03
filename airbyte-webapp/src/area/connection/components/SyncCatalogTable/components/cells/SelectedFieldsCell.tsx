import { Row } from "@tanstack/react-table";
import React from "react";

import { Text } from "components/ui/Text";

import { SyncCatalogUIModel } from "../../SyncCatalogTable";

interface FieldSelectionStatusCellProps {
  row: Row<SyncCatalogUIModel>;
}

export const SelectedFieldsCell: React.FC<FieldSelectionStatusCellProps> = ({ row }) => {
  const { streamNode, traversedFields } = row.original;

  const totalFieldCount = traversedFields?.length || 0;
  const selectedFieldCount = streamNode?.config?.fieldSelectionEnabled
    ? streamNode?.config?.selectedFields?.length
    : totalFieldCount;

  return streamNode?.config?.selected ? (
    <Text>
      {selectedFieldCount}/{totalFieldCount}
    </Text>
  ) : null;
};
