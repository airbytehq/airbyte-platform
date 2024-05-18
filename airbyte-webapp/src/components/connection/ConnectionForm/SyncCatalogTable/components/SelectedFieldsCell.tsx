import { Row } from "@tanstack/react-table";
import React from "react";

import { Text } from "components/ui/Text";

import { SyncCatalogUIModel } from "../SyncCatalogTable";

interface FieldSelectionStatusCellProps {
  row: Row<SyncCatalogUIModel>;
}

export const SelectedFieldsCell: React.FC<FieldSelectionStatusCellProps> = ({ row }) => {
  const {
    streamNode: { config },
    traversedFields,
  } = row.original;

  const totalFieldCount = traversedFields?.length || 0;
  const selectedFieldCount = config?.fieldSelectionEnabled
    ? config?.selectedFields?.length === 0
      ? totalFieldCount
      : config?.selectedFields?.length
    : totalFieldCount;

  return (
    <Text>
      {selectedFieldCount}/{totalFieldCount}
    </Text>
  );
};
