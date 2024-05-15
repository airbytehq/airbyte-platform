import { Row } from "@tanstack/react-table";
import React from "react";

import { Text } from "components/ui/Text";

import { SyncCatalogUIModel } from "../SyncCatalogTable";

interface FieldSelectionStatusCellProps {
  row: Row<SyncCatalogUIModel>;
}

export const SelectedFieldsCell: React.FC<FieldSelectionStatusCellProps> = ({ row }) => {
  const { streamNode, traversedFields } = row.original;

  const selectedFieldCount = streamNode.config?.selectedFields?.length ?? (traversedFields?.length || 0);
  const totalFieldCount = traversedFields?.length || 0;

  return (
    <Text>
      {selectedFieldCount}/{totalFieldCount}
    </Text>
  );
};
