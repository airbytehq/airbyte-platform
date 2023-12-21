import {
  ColumnDef,
  flexRender,
  useReactTable,
  VisibilityState,
  Row,
  getSortedRowModel,
  getCoreRowModel,
} from "@tanstack/react-table";
import classNames from "classnames";
import React, { PropsWithChildren } from "react";
import { TableVirtuoso, TableComponents, ItemProps } from "react-virtuoso";

import { SortableTableHeader } from "./SortableTableHeader";
import styles from "./Table.module.scss";
import { ColumnMeta } from "./types";

// We can leave type any here since useReactTable options.columns itself is waiting for Array<ColumnDef<T, any>>
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type TableColumns<T> = Array<ColumnDef<T, any>>;

export interface TableProps<T> {
  className?: string;
  columns: TableColumns<T>;
  data: T[];
  variant?: "default" | "light" | "white" | "inBlock";
  getRowCanExpand?: (data: Row<T>) => boolean;
  getIsRowExpanded?: (data: Row<T>) => boolean;
  expandedRow?: (props: { row: Row<T> }) => React.ReactElement;
  testId?: string;
  columnVisibility?: VisibilityState;
  sorting?: boolean;
  getRowClassName?: (data: T) => string | undefined;
  initialSortBy?: [{ id: string; desc: boolean }];
  /**
   * If true, the table will be rendered using react-virtuoso. Defaults to false.
   */
  virtualized?: boolean;
  /**
   * Props to be passed to react-virtuoso. See https://virtuoso.dev/ for more details.
   */
  virtualizedProps?: Omit<
    React.ComponentProps<typeof TableVirtuoso>,
    "data" | "components" | "totalCount" | "fixedHeaderContent"
  >;
}

export const Table = <T,>({
  testId,
  className,
  columns,
  data,
  variant = "default",
  getRowCanExpand,
  getIsRowExpanded,
  expandedRow,
  columnVisibility,
  getRowClassName,
  sorting = true,
  initialSortBy,
  virtualized = false,
  virtualizedProps,
}: PropsWithChildren<TableProps<T>>) => {
  const table = useReactTable({
    columns,
    data,
    initialState: {
      columnVisibility,
      sorting: initialSortBy,
    },
    getCoreRowModel: getCoreRowModel<T>(),
    getSortedRowModel: getSortedRowModel<T>(),
    getRowCanExpand,
    getIsRowExpanded,
    enableSorting: sorting,
  });

  const rows = table.getRowModel().rows;

  const Table: TableComponents["Table"] = ({ style, ...props }) => (
    <table
      className={classNames(styles.table, className, {
        [styles["table--default"]]: variant === "default",
      })}
      {...props}
      style={style}
      data-testid={testId}
    />
  );

  const TableHead: TableComponents["TableHead"] = React.forwardRef((props, ref) => (
    <thead ref={ref} className={styles.thead} {...props} />
  ));
  TableHead.displayName = "TableHead";

  const TableRow: React.FC<{ row: Row<T>; restRowProps?: ItemProps<T> }> = ({ row, restRowProps }) => {
    return (
      <>
        <tr
          className={classNames(styles.tr, getRowClassName?.(row.original))}
          data-testid={`table-row-${row.id}`}
          {...(virtualized && { ...restRowProps })}
        >
          {row.getVisibleCells().map((cell) => {
            const meta = cell.column.columnDef.meta as ColumnMeta | undefined;
            return (
              <td
                className={classNames(styles.td, meta?.tdClassName, {
                  [styles["td--responsive"]]: meta?.responsive,
                  [styles["td--noPadding"]]: meta?.noPadding,
                })}
                key={`table-cell-${row.id}-${cell.id}`}
                data-testid={`table-cell-${row.id}-${cell.id}`}
              >
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </td>
            );
          })}
        </tr>
        {row.getIsExpanded() && expandedRow ? (
          <tr>
            <td colSpan={row.getVisibleCells().length}>{expandedRow({ row })}</td>
          </tr>
        ) : null}
      </>
    );
  };

  // virtuoso uses "data-index" to identify the row, hence we need additional wrapper specifically for virtuoso
  const TableRowVirtualized: React.FC<ItemProps<T>> = (props) => {
    const index = props["data-index"];
    const row = rows[index];

    return <TableRow row={row} restRowProps={props} />;
  };

  const headerContent = () =>
    table.getHeaderGroups().map((headerGroup) => (
      <tr key={`table-header-${headerGroup.id}}`}>
        {headerGroup.headers.map((header) => {
          const meta = header.column.columnDef.meta as ColumnMeta | undefined;
          const isSorted = header.column.getIsSorted();
          return (
            <th
              colSpan={header.colSpan}
              className={classNames(
                styles.th,
                {
                  [styles["th--default"]]: variant === "default",
                  [styles["th--light"]]: variant === "light",
                  [styles["th--white"]]: variant === "white",
                  [styles["th--inBlock"]]: variant === "inBlock",
                  [styles["th--sorted"]]: isSorted,
                },
                meta?.thClassName
              )}
              key={`table-column-${headerGroup.id}-${header.id}`}
            >
              {header.column.getCanSort() === true ? (
                <SortableTableHeader
                  onClick={() => header.column.toggleSorting()}
                  isActive={header.column.getIsSorted() !== false}
                  isAscending={header.column.getIsSorted() === "asc"}
                >
                  {flexRender(header.column.columnDef.header, header.getContext())}
                </SortableTableHeader>
              ) : (
                flexRender(header.column.columnDef.header, header.getContext())
              )}
            </th>
          );
        })}
      </tr>
    ));

  return virtualized ? (
    <TableVirtuoso<T>
      // the parent container should have exact height to make "AutoSizer" work properly
      style={{ height: "100%" }}
      totalCount={rows.length}
      {...virtualizedProps}
      components={{
        Table,
        TableHead,
        TableRow: TableRowVirtualized,
      }}
      fixedHeaderContent={headerContent}
    />
  ) : (
    <table
      className={classNames(styles.table, className, {
        [styles["table--default"]]: variant === "default",
      })}
      data-testid={testId}
    >
      <thead className={styles.thead}>{headerContent()}</thead>
      <tbody>
        {rows.map((row) => (
          <TableRow key={`table-row-${row.id}`} row={row} />
        ))}
      </tbody>
    </table>
  );
};
