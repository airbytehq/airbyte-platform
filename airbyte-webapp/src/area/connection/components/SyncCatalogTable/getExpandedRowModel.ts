/* eslint-disable @typescript-eslint/no-explicit-any */
import { Table, Row, RowModel, TableOptionsResolved, memo } from "@tanstack/react-table";

import { SyncCatalogUIModel } from "./SyncCatalogTable";

export function getMemoOptions(
  tableOptions: Partial<TableOptionsResolved<any>>,
  debugLevel: "debugAll" | "debugTable" | "debugColumns" | "debugRows" | "debugHeaders",
  key: string,
  onChange?: (result: any) => void
) {
  return {
    debug: () => tableOptions?.debugAll ?? tableOptions[debugLevel],
    key: process.env.NODE_ENV === "development" && key,
    onChange,
  };
}

/**
 * A custom implementation of getExpandedRowModel() from tanstack-table.
 * Compare with original implementation at: https://github.com/TanStack/table/blob/7fe650d666cfc3807b8408a1205bc1686a479cdb/packages/table-core/src/utils/getExpandedRowModel.ts
 */
export function getExpandedRowModel(): (table: Table<SyncCatalogUIModel>) => () => RowModel<SyncCatalogUIModel> {
  return (table) =>
    memo(
      () => [table.getState().expanded, table.getPreExpandedRowModel(), table.options.paginateExpandedRows],
      (expanded, rowModel) => {
        if (!rowModel.rows.length || (expanded !== true && !Object.keys(expanded ?? {}).length)) {
          return rowModel;
        }

        return expandRows(rowModel, table.getState().globalFilter, table);
      },
      getMemoOptions(table.options, "debugTable", "getExpandedRowModel")
    );
}

export function expandRows(
  rowModel: RowModel<SyncCatalogUIModel>,
  globalFilter: string,
  table: Table<SyncCatalogUIModel>
) {
  const maxDepth = table.options.maxLeafRowFilterDepth ?? 100;
  const expandedRows: Array<Row<SyncCatalogUIModel>> = [];

  const recurseMatchingSubRows = (subRows: Array<Row<SyncCatalogUIModel>>): Array<Row<SyncCatalogUIModel>> => {
    const matchingRows: Array<Row<SyncCatalogUIModel>> = [];

    subRows.forEach((row) => {
      if (rowHasSearchTerm(row, globalFilter) && row.depth < maxDepth) {
        matchingRows.push(row);
      }
      if (row.subRows?.length) {
        const matchingSubrows = recurseMatchingSubRows(row.subRows);
        // If any subrows match, include the parent row and the matching subrows
        if (matchingSubrows.length) {
          matchingRows.push(row);
          matchingRows.push(...matchingSubrows);
        }
      }
    });

    return matchingRows;
  };

  const handleRow = (row: Row<SyncCatalogUIModel>) => {
    expandedRows.push(row);

    if (row.subRows?.length && row.getIsExpanded()) {
      row.subRows.forEach(handleRow);
      // Even if the parent is not expanded, we need to iterate through the subRows to look for rows that match the search
    } else if (row.subRows?.length && globalFilter !== "" && !row.getIsExpanded()) {
      expandedRows.push(...recurseMatchingSubRows(row.subRows));
    }
  };

  rowModel.rows.forEach(handleRow);

  return {
    rows: expandedRows,
    flatRows: rowModel.flatRows,
    rowsById: rowModel.rowsById,
  };
}

function rowHasSearchTerm(row: Row<SyncCatalogUIModel>, globalFilter: string) {
  return row.original.name.toLocaleLowerCase().includes(globalFilter.toLocaleLowerCase());
}
