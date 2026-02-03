import { Table, RowModel, Row, ResolvedColumnFilter, memo } from "@tanstack/react-table";

import { filterRows } from "./filterRowsUtils";
import { SyncCatalogUIModel } from "./SyncCatalogTable";

/**
 * This method is mostly unchanged from the original implementation:
 * https://github.com/TanStack/table/blob/v8.7.0/packages/table-core/src/utils/getFilteredRowModel.ts
 *
 * The changes:
 *  - We are importing our own customized filterRows() util method
 *  - The generic typings have been replaced with <SyncCatalogUIModel> so we can access specific fields we know exist
 */
export function getFilteredRowModel(): (table: Table<SyncCatalogUIModel>) => () => RowModel<SyncCatalogUIModel> {
  return (table) =>
    memo(
      () => [
        table.getPreFilteredRowModel(),
        table.getState().columnFilters,
        table.getState().globalFilter,
        table.options.maxLeafRowFilterDepth,
      ],
      (rowModel, columnFilters, globalFilter) => {
        if (!rowModel.rows.length || (!columnFilters?.length && !globalFilter)) {
          for (let i = 0; i < rowModel.flatRows.length; i++) {
            rowModel.flatRows[i].columnFilters = {};
            rowModel.flatRows[i].columnFiltersMeta = {};
          }
          return rowModel;
        }

        const resolvedColumnFilters: Array<ResolvedColumnFilter<SyncCatalogUIModel>> = [];
        const resolvedGlobalFilters: Array<ResolvedColumnFilter<SyncCatalogUIModel>> = [];

        (columnFilters ?? []).forEach((d) => {
          const column = table.getColumn(d.id);

          if (!column) {
            if (process.env.NODE_ENV !== "production") {
              console.warn(`Table: Could not find a column to filter with columnId: ${d.id}`);
            }
          }

          const filterFn = column.getFilterFn();

          if (!filterFn) {
            if (process.env.NODE_ENV !== "production") {
              console.warn(`Could not find a valid 'column.filterFn' for column with the ID: ${column.id}.`);
            }
            return;
          }

          resolvedColumnFilters.push({
            id: d.id,
            filterFn,
            resolvedValue: filterFn.resolveFilterValue?.(d.value) ?? d.value,
          });
        });

        const filterableIds = columnFilters.map((d) => d.id);

        const globalFilterFn = table.getGlobalFilterFn();

        const globallyFilterableColumns = table.getAllLeafColumns().filter((column) => column.getCanGlobalFilter());

        if (globalFilter && globalFilterFn && globallyFilterableColumns.length) {
          filterableIds.push("__global__");

          globallyFilterableColumns.forEach((column) => {
            resolvedGlobalFilters.push({
              id: column.id,
              filterFn: globalFilterFn,
              resolvedValue: globalFilterFn.resolveFilterValue?.(globalFilter) ?? globalFilter,
            });
          });
        }

        let currentColumnFilter;
        let currentGlobalFilter;

        // Flag the prefiltered row model with each filter state
        for (let j = 0; j < rowModel.flatRows.length; j++) {
          const row = rowModel.flatRows[j];

          row.columnFilters = {};

          if (resolvedColumnFilters.length) {
            for (let i = 0; i < resolvedColumnFilters.length; i++) {
              currentColumnFilter = resolvedColumnFilters[i]!;
              const id = currentColumnFilter.id;

              // Tag the row with the column filter state
              row.columnFilters[id] = currentColumnFilter.filterFn(
                row,
                id,
                currentColumnFilter.resolvedValue,
                (filterMeta) => {
                  row.columnFiltersMeta[id] = filterMeta;
                }
              );
            }
          }

          if (resolvedGlobalFilters.length) {
            for (let i = 0; i < resolvedGlobalFilters.length; i++) {
              currentGlobalFilter = resolvedGlobalFilters[i]!;
              const id = currentGlobalFilter.id;
              // Tag the row with the first truthy global filter state
              if (
                currentGlobalFilter.filterFn(row, id, currentGlobalFilter.resolvedValue, (filterMeta) => {
                  row.columnFiltersMeta[id] = filterMeta;
                })
              ) {
                row.columnFilters.__global__ = true;
                break;
              }
            }

            if (row.columnFilters.__global__ !== true) {
              row.columnFilters.__global__ = false;
            }
          }
        }

        const filterRowsImpl = (row: Row<SyncCatalogUIModel>) => {
          // Horizontally filter rows through each column
          for (let i = 0; i < filterableIds.length; i++) {
            if (row.columnFilters[filterableIds[i]] === false) {
              return false;
            }
          }
          return true;
        };

        // Filter final rows using all of the active filters
        return filterRows(rowModel.rows, filterRowsImpl, table);
      },
      {
        key: process.env.NODE_ENV === "development" && "getFilteredRowModel",
        debug: () => table.options.debugAll ?? table.options.debugTable,
        onChange: () => {
          table._autoResetPageIndex();
        },
      }
    );
}
