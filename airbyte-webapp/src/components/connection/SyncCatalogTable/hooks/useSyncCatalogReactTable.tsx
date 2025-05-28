import {
  ColumnDef,
  ColumnFiltersState,
  ExpandedState,
  getCoreRowModel,
  getGroupedRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";

import { getExpandedRowModel } from "../getExpandedRowModel";
import { getFilteredRowModel } from "../getFilteredRowModel";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

interface UseSyncCatalogReactTableProps {
  columns: Array<ColumnDef<SyncCatalogUIModel>>;
  data: SyncCatalogUIModel[];
  expanded: ExpandedState;
  setExpanded: React.Dispatch<React.SetStateAction<ExpandedState>>;
  globalFilter: string;
  globalFilterMaxDepth?: number;
  columnFilters: ColumnFiltersState;
  setColumnFilters: React.Dispatch<React.SetStateAction<ColumnFiltersState>>;
  showHashing: boolean;
}

export const useSyncCatalogReactTable = ({
  columns,
  data,
  expanded,
  setExpanded,
  globalFilter,
  globalFilterMaxDepth,
  columnFilters,
  setColumnFilters,
  showHashing,
}: UseSyncCatalogReactTableProps) =>
  useReactTable<SyncCatalogUIModel>({
    columns,
    data,
    getSubRows: (row) => row.subRows,
    state: {
      expanded,
      globalFilter,
      columnFilters,
      columnVisibility: {
        "stream.selected": false,
        hashing: showHashing,
      },
    },
    initialState: {
      sorting: [{ id: "stream.name", desc: false }],
    },
    autoResetExpanded: false,
    filterFromLeafRows: true,
    enableGlobalFilter: true,
    maxLeafRowFilterDepth: globalFilterMaxDepth,
    getExpandedRowModel: getExpandedRowModel(),
    getGroupedRowModel: getGroupedRowModel(),
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    onExpandedChange: setExpanded,
    onColumnFiltersChange: setColumnFilters,
    getRowCanExpand: (row) => !!row.subRows.length,
  });
