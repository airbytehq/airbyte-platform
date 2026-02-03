/* eslint-disable @typescript-eslint/no-explicit-any */
import { RowModel, Table, createRow, Row, RowData } from "@tanstack/react-table";

import { SyncCatalogUIModel } from "./SyncCatalogTable";

/**
 * This method is unchanged from the original implementation, except for removing the generic typings:
 * https://github.com/TanStack/table/blob/v8.7.0/packages/table-core/src/utils/filterRowsUtils.ts#L4C1-L14C2
 */
export function filterRows(
  rows: Array<Row<SyncCatalogUIModel>>,
  filterRowImpl: (row: Row<SyncCatalogUIModel>) => any,
  table: Table<SyncCatalogUIModel>
) {
  if (table.options.filterFromLeafRows) {
    return filterRowModelFromLeafs(rows, filterRowImpl, table);
  }

  return filterRowModelFromRoot(rows, filterRowImpl, table);
}

/**
 * This method has been customized to always include sibling fields of a matching row. Original implementation:
 * https://github.com/TanStack/table/blob/v8.7.0/packages/table-core/src/utils/filterRowsUtils.ts#L16-L76
 */
function filterRowModelFromLeafs(
  rowsToFilter: Array<Row<SyncCatalogUIModel>>,
  filterRow: (row: Row<SyncCatalogUIModel>) => Array<Row<SyncCatalogUIModel>>,
  table: Table<SyncCatalogUIModel>
): RowModel<SyncCatalogUIModel> {
  const maxDepth = table.options.maxLeafRowFilterDepth ?? 100;

  const recurseFilterRows = (rowsToFilter: Array<Row<SyncCatalogUIModel>>, depth = 0) => {
    const rows: Array<Row<SyncCatalogUIModel>> = [];

    for (let i = 0; i < rowsToFilter.length; i++) {
      const row = rowsToFilter[i];

      const newRow = createRow(table, row.id, row.original, row.index, row.depth);
      newRow.columnFilters = row.columnFilters;

      if (row.subRows?.length && depth < maxDepth) {
        // Recursing again here to do a depth first search (deep leaf nodes first)
        newRow.subRows = recurseFilterRows(row.subRows, depth + 1);

        // If a stream is a match, include all its subrows
        if (newRow.original.rowType === "stream" && filterRow(newRow)) {
          newRow.subRows = rowsToFilter[i].subRows;
          rows.push(newRow);
          continue;
        }

        // If a stream contains at least one match, include all its subrows
        if (newRow.original.rowType === "stream" && newRow.subRows.length) {
          newRow.subRows = rowsToFilter[i].subRows;
          rows.push(newRow);
          continue;
        }

        if (filterRow(newRow) && !newRow.subRows.length) {
          rows.push(newRow);
          continue;
        }

        if (filterRow(newRow) || newRow.subRows.length) {
          rows.push(newRow);
          continue;
        }
      } else if (filterRow(newRow)) {
        newRow.subRows = rowsToFilter[i].subRows;
        rows.push(newRow);
      }
    }

    return rows;
  };

  const filteredRows: Array<Row<SyncCatalogUIModel>> = recurseFilterRows(rowsToFilter);
  const filteredRowsById: Record<string, Row<SyncCatalogUIModel>> = {};

  function flattenRows(rows: Array<Row<SyncCatalogUIModel>>) {
    const flattenedRows: Array<Row<SyncCatalogUIModel>> = [];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      flattenedRows.push(row);
      filteredRowsById[row.id] = row;
      if (row.subRows) {
        flattenedRows.push(...flattenRows(row.subRows));
      }
    }
    return flattenedRows;
  }

  return {
    rows: filteredRows,
    rowsById: filteredRowsById,
    flatRows: flattenRows(filteredRows),
  };
}

/**
 * This method is unchanged from the original implementation:
 * https://github.com/TanStack/table/blob/v8.7.0/packages/table-core/src/utils/filterRowsUtils.ts#L78-L126
 */
export function filterRowModelFromRoot<TData extends RowData>(
  rowsToFilter: Array<Row<TData>>,
  filterRow: (row: Row<TData>) => any,
  table: Table<TData>
): RowModel<TData> {
  const newFilteredFlatRows: Array<Row<TData>> = [];
  const newFilteredRowsById: Record<string, Row<TData>> = {};
  const maxDepth = table.options.maxLeafRowFilterDepth ?? 100;

  // Filters top level and nested rows
  const recurseFilterRows = (rowsToFilter: Array<Row<TData>>, depth = 0) => {
    // Filter from parents downward first

    const rows = [];

    // Apply the filter to any subRows
    for (let i = 0; i < rowsToFilter.length; i++) {
      let row = rowsToFilter[i];

      const pass = filterRow(row);

      if (pass) {
        if (row.subRows?.length && depth < maxDepth) {
          const newRow = createRow(table, row.id, row.original, row.index, row.depth);
          newRow.subRows = recurseFilterRows(row.subRows, depth + 1);
          row = newRow;
        }

        rows.push(row);
        newFilteredFlatRows.push(row);
        newFilteredRowsById[row.id] = row;
      }
    }

    return rows;
  };

  return {
    rows: recurseFilterRows(rowsToFilter),
    flatRows: newFilteredFlatRows,
    rowsById: newFilteredRowsById,
  };
}
